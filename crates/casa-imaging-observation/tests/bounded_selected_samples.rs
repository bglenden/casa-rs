// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AntennaSelection, AxisOrder, CentreLaws, ColumnGeneration, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame, FacetLayout,
    FiniteValuePolicy, FlagPolicy, FrequencyFrame, GeometryInput, IdSelection, ImageAxis,
    ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest, InstrumentResponse,
    IntentSelection, LogicalIdentity, MeasurementEquationContract, MeasurementSetIdentity,
    MetadataGeneration, MetadataTableKind, MissingPointingPolicy, ModelColumnState,
    ModelColumnWrite, ModelInnerProduct, ModelStateIdentity, MsColumnKind, NumericPrecision,
    NumericalStage, NumericsContract, ObservationPointingLaw, ObservationSelection,
    ObservationSnapshotInput, ObservationSourceInput, ObservationSourceProvenance,
    ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw, PointingDirectionColumn,
    PointingDirectionSemantic, PointingExtrapolation, PointingInterpolation, PointingTimeSampling,
    PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy,
    ProblemInputIdentities, ProblemSpecification, ProductBlankingPolicy, ProductKind,
    ProductNormalization, ProductRequirements, ProductSupportComparison, ProductValidityPolicies,
    Projection, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract,
    ReconstructionControls, ReductionPolicy, ReferenceDataKind, RestFrequency, RestoringBeamPolicy,
    RowSelection, ScientificContract, SelectedColumns, SelectedMainRow, SelectedRows,
    SelectedVisibilitySample, SkyDirection, SourceGenerations, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSampling, SpectralWcs,
    SpectralWindowSelection, StageErrorBudget, TaylorSupportReference, TaylorValidityPolicy,
    TimeSelection, UvSelection, UvwCoordinateLaw, VisibilityColumn, VisibilityInnerProduct,
    WeightColumn, WeightDensityScope, WeightingContract, WeightingScheme, compile,
    compile_observation,
};
use casa_imaging_observation::{
    BoundObservationSource, BoundSelectedObservation, ObservationSourceReadPlan,
    SelectedObservationContentBudget,
};
use casa_ms::subtables::SubTable;
use casa_ms::{
    MeasurementSet, MsReadPlan, MsSelectionIoBudget, SyntheticObservationRequest,
    SyntheticSpectralSetup, SyntheticWorkerPolicy, generate_synthetic_observation_ms,
    tutorial_vla_a_antennas,
};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;

#[test]
fn retained_selected_samples_are_bounded_and_block_partition_invariant() {
    let directory = tempfile::tempdir().expect("temporary selected-observation fixture");
    let path = directory.path().join("selected.ms");
    generate_fixture(&path);

    let problem = compiled_problem(&path, 2);
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let one_row = BoundObservationSource::open(source, read_plan(2, 1), content_budget(1_024))
        .expect("bind one-row physical blocks");
    let two_rows = BoundObservationSource::open(source, read_plan(2, 2), content_budget(2_048))
        .expect("bind two-row physical blocks");
    assert_eq!(one_row.content_plan().rows_per_block(), 1);
    assert_eq!(two_rows.content_plan().rows_per_block(), 2);
    assert_eq!(
        one_row.content_plan().bytes_per_row(),
        two_rows.content_plan().bytes_per_row()
    );
    assert_eq!(one_row.content_plan().bytes_per_row(), 955);
    assert!(one_row.content_plan().bytes_per_block() <= 1_024);
    assert!(two_rows.content_plan().bytes_per_block() <= 2_048);

    let one_row_samples = one_row
        .selected_samples(&problem)
        .expect("prepare one-row selected stream")
        .collect::<Result<Vec<_>, _>>()
        .expect("read one-row selected stream");
    let two_row_samples = two_rows
        .selected_samples(&problem)
        .expect("prepare two-row selected stream")
        .collect::<Result<Vec<_>, _>>()
        .expect("read two-row selected stream");

    assert_eq!(one_row_samples.len(), 8);
    assert_eq!(one_row_samples, two_row_samples);
    assert_eq!(
        one_row_samples
            .iter()
            .map(|sample| {
                (
                    sample.address.physical_row,
                    sample.address.channel_index,
                    sample.address.correlation_index,
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
    let first = one_row_samples[0];
    assert_eq!(first.address.frequency_centre_hz, 1.4e9);
    assert_eq!(first.address.frequency_lower_hz, 1.3995e9);
    assert_eq!(first.address.frequency_upper_hz, 1.4005e9);
    assert_eq!(first.address.channel_width_hz, 1.0e6);
    assert_eq!(first.address.frequency_frame, FrequencyFrame::Topocentric);
    assert_eq!(first.address.correlation_type, CorrelationType::CircularRr);
    assert_eq!(
        first.visibility,
        SelectedVisibilitySample::Complex32([0.0, 0.0])
    );
    assert_eq!(first.coordinates.density_uvw_m, first.coordinates.raw_uvw_m);
    assert_eq!(
        first.coordinates.transformed_uvw_m,
        first.coordinates.raw_uvw_m
    );
    assert_eq!(first.coordinates.phase_shift_m, 0.0);
    assert_eq!(
        first.coordinates.phase_direction,
        first.coordinates.delay_direction
    );
    assert_eq!(
        first.coordinates.phase_direction,
        first.coordinates.pointing_directions.antenna1
    );
    assert_eq!(
        first.coordinates.phase_direction.frame(),
        DirectionFrame::J2000
    );
    assert_eq!(first.metadata.antenna1, 0);
    assert_eq!(first.metadata.antenna2, 1);
    assert_eq!(first.metadata.feed1, 0);
    assert_eq!(first.metadata.feed2, 0);
    assert_eq!(
        problem
            .inspect_selected_observation(one_row_samples)
            .expect("inspect exact bounded stream"),
        problem
            .inspect_selected_observation(two_row_samples)
            .expect("inspect repartitioned bounded stream")
    );
}

#[test]
fn selected_observation_residency_and_identity_are_invariant_to_rows_and_double_buffering() {
    let directory = tempfile::tempdir().expect("temporary residency fixtures");
    let small_path = directory.path().join("small.ms");
    let large_path = directory.path().join("large.ms");
    generate_fixture_with_rows(&small_path, 2);
    generate_fixture_with_rows(&large_path, 64);

    let small_problem = compiled_problem(&small_path, 2);
    let small_source = &small_problem.inputs().observation_snapshot().sources()[0];
    let synchronous = BoundObservationSource::open(
        small_source,
        read_plan(2, 2),
        content_budget_with_live_blocks(2_048, 1),
    )
    .expect("bind synchronous selected observation");
    let double_buffered = BoundObservationSource::open(
        small_source,
        read_plan(2, 2),
        content_budget_with_live_blocks(2_048, 2),
    )
    .expect("bind double-buffered selected observation");
    assert_eq!(synchronous.content_plan().rows_per_block(), 2);
    assert_eq!(double_buffered.content_plan().rows_per_block(), 1);
    assert_eq!(synchronous.content_plan().maximum_live_blocks(), 1);
    assert_eq!(double_buffered.content_plan().maximum_live_blocks(), 2);
    assert!(
        synchronous.content_plan().bytes_per_block()
            * synchronous.content_plan().maximum_live_blocks()
            <= 2_048
    );
    assert!(
        double_buffered.content_plan().bytes_per_block()
            * double_buffered.content_plan().maximum_live_blocks()
            <= 2_048
    );

    let synchronous_samples = synchronous
        .selected_samples(&small_problem)
        .expect("prepare synchronous replay")
        .collect::<Result<Vec<_>, _>>()
        .expect("read synchronous replay");
    let double_buffered_samples = double_buffered
        .selected_samples(&small_problem)
        .expect("prepare double-buffered replay")
        .collect::<Result<Vec<_>, _>>()
        .expect("read double-buffered replay");
    assert_eq!(synchronous_samples, double_buffered_samples);
    assert_eq!(
        small_problem
            .inspect_selected_observation(synchronous_samples)
            .expect("inspect synchronous replay"),
        small_problem
            .inspect_selected_observation(double_buffered_samples)
            .expect("inspect double-buffered replay")
    );

    let large_problem = compiled_problem(&large_path, 64);
    let large_source = &large_problem.inputs().observation_snapshot().sources()[0];
    let large = BoundObservationSource::open(
        large_source,
        read_plan(64, 2),
        content_budget_with_live_blocks(2_048, 1),
    )
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
        large
            .selected_samples(&large_problem)
            .expect("prepare large bounded replay")
            .try_fold(0_usize, |count, sample| sample.map(|_| count + 1))
            .expect("read large bounded replay"),
        64 * 2 * 2
    );
}

#[test]
fn retained_selected_observation_owns_canonical_multi_source_order() {
    let directory = tempfile::tempdir().expect("temporary multi-source fixture");
    let first_path = directory.path().join("first.ms");
    let second_path = directory.path().join("second.ms");
    generate_fixture(&first_path);
    generate_fixture(&second_path);
    let problem = compiled_problem_with_sources(&[(&first_path, 1, 2), (&second_path, 2, 2)]);
    let sources = problem.inputs().observation_snapshot().sources();
    let one_row_plans = sources
        .iter()
        .map(|source| {
            ObservationSourceReadPlan::new(
                source.identity(),
                read_plan(2, 1),
                content_budget(1_024),
            )
        })
        .collect();
    let two_row_plans = sources
        .iter()
        .rev()
        .map(|source| {
            ObservationSourceReadPlan::new(
                source.identity(),
                read_plan(2, 2),
                content_budget(2_048),
            )
        })
        .collect();
    let one_row = BoundSelectedObservation::open(&problem, one_row_plans)
        .expect("bind canonical multi-source observation");
    let two_rows = BoundSelectedObservation::open(&problem, two_row_plans)
        .expect("bind reordered physical source plans by typed identity");

    let one_row_samples = one_row
        .selected_samples(&problem)
        .expect("prepare canonical multi-source stream")
        .collect::<Result<Vec<_>, _>>()
        .expect("read canonical multi-source stream");
    let two_row_samples = two_rows
        .selected_samples(&problem)
        .expect("prepare repartitioned multi-source stream")
        .collect::<Result<Vec<_>, _>>()
        .expect("read repartitioned multi-source stream");

    assert_eq!(one_row_samples.len(), 16);
    assert_eq!(one_row_samples, two_row_samples);
    assert_eq!(
        one_row_samples
            .chunks_exact(8)
            .map(|samples| samples[0].address.measurement_set)
            .collect::<Vec<_>>(),
        problem
            .selected_observation()
            .read_set()
            .sources()
            .iter()
            .map(|source| source.measurement_set())
            .collect::<Vec<_>>()
    );
    problem
        .inspect_selected_observation(one_row_samples)
        .expect("inspect canonical multi-source stream");
}

#[test]
fn retained_selected_samples_evaluate_fixed_centres_and_uvw_coordinates() {
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
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let bound = BoundObservationSource::open(source, read_plan(2, 2), content_budget(2_048))
        .expect("bind fixed-centre source");
    let samples = bound
        .selected_samples(&problem)
        .expect("prepare fixed-centre stream")
        .collect::<Result<Vec<_>, _>>()
        .expect("evaluate fixed-centre samples");

    assert_eq!(samples.len(), 8);
    for sample in &samples {
        assert_eq!(sample.coordinates.phase_direction, phase);
        assert_eq!(sample.coordinates.delay_direction, delay);
        assert_eq!(sample.coordinates.pointing_directions.antenna1, pointing);
        assert_eq!(sample.coordinates.pointing_directions.antenna2, pointing);
        assert_ne!(
            sample.coordinates.transformed_uvw_m,
            sample.coordinates.raw_uvw_m
        );
        assert_ne!(
            sample.coordinates.density_uvw_m,
            sample.coordinates.raw_uvw_m
        );
        assert_ne!(sample.coordinates.phase_shift_m, 0.0);
    }
    problem
        .inspect_selected_observation(samples)
        .expect("inspect fixed-centre stream");
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
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let one_row = BoundObservationSource::open(source, read_plan(2, 1), content_budget(1_024))
        .expect("bind one-row POINTING stream");
    let two_rows = BoundObservationSource::open(source, read_plan(2, 2), content_budget(2_048))
        .expect("bind two-row POINTING stream");
    let one_row_samples = one_row
        .selected_samples(&problem)
        .expect("prepare one-row POINTING stream")
        .collect::<Result<Vec<_>, _>>()
        .expect("read one-row POINTING stream");
    let two_row_samples = two_rows
        .selected_samples(&problem)
        .expect("prepare two-row POINTING stream")
        .collect::<Result<Vec<_>, _>>()
        .expect("read two-row POINTING stream");

    assert_eq!(one_row_samples, two_row_samples);
    for sample in &one_row_samples {
        assert_eq!(
            sample.coordinates.pointing_directions.antenna1,
            SkyDirection::new(
                DirectionFrame::J2000,
                antenna1_pointing[0],
                antenna1_pointing[1],
            )
        );
        assert_eq!(
            sample.coordinates.pointing_directions.antenna2,
            SkyDirection::new(
                DirectionFrame::J2000,
                antenna2_pointing[0],
                antenna2_pointing[1],
            )
        );
    }
    problem
        .inspect_selected_observation(one_row_samples)
        .expect("inspect exact bounded POINTING stream");
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
    let source = &fallback_problem.inputs().observation_snapshot().sources()[0];
    let fallback = BoundObservationSource::open(source, read_plan(2, 2), content_budget(2_048))
        .expect("bind fallback POINTING source")
        .selected_samples(&fallback_problem)
        .expect("prepare fallback POINTING stream")
        .collect::<Result<Vec<_>, _>>()
        .expect("explicitly fall back to phase centre");
    for sample in fallback {
        assert_eq!(
            sample.coordinates.pointing_directions.antenna1,
            sample.coordinates.phase_direction
        );
        assert_eq!(
            sample.coordinates.pointing_directions.antenna2,
            sample.coordinates.phase_direction
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
    let source = &rejecting_problem.inputs().observation_snapshot().sources()[0];
    let error = BoundObservationSource::open(source, read_plan(2, 2), content_budget(2_048))
        .expect("bind rejecting POINTING source")
        .selected_samples(&rejecting_problem)
        .expect("prepare rejecting POINTING stream")
        .collect::<Result<Vec<_>, _>>()
        .expect_err("missing required POINTING must fail closed");
    assert!(matches!(
        error,
        casa_imaging_observation::BoundObservationSourceError::MissingPointingDirection { .. }
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
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let samples = BoundObservationSource::open(source, read_plan(2, 2), content_budget(2_048))
        .expect("bind interpolated POINTING source")
        .selected_samples(&problem)
        .expect("prepare interpolated POINTING stream")
        .collect::<Result<Vec<_>, _>>()
        .expect("interpolate POINTING directions");

    for sample in &samples[..4] {
        assert!(
            (sample
                .coordinates
                .pointing_directions
                .antenna1
                .longitude_rad()
                - 0.05)
                .abs()
                < 1.0e-12
        );
        assert!(
            (sample
                .coordinates
                .pointing_directions
                .antenna2
                .longitude_rad()
                - 0.45)
                .abs()
                < 1.0e-12
        );
    }
    for sample in &samples[4..] {
        assert!(
            (sample
                .coordinates
                .pointing_directions
                .antenna1
                .longitude_rad()
                - 0.15)
                .abs()
                < 1.0e-12
        );
        assert!(
            (sample
                .coordinates
                .pointing_directions
                .antenna2
                .longitude_rad()
                - 0.55)
                .abs()
                < 1.0e-12
        );
    }
}

#[test]
fn multi_spw_selection_is_block_invariant_across_prediction_and_residual_replays() {
    let directory = tempfile::tempdir().expect("temporary multi-SPW fixture");
    let path = directory.path().join("multi-spw.ms");
    generate_fixture(&path);
    extend_fixture_with_second_spw(&path);
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![multi_spw_source_input(&path, 1)],
        vec![(ReferenceDataKind::Measures, identity(90))],
        ModelStateIdentity::Empty,
    ))
    .expect("compile multi-SPW observation");
    let problem = compile(ImagingRequest::new(
        specification(),
        geometry(),
        ProblemInputIdentities::new(snapshot),
    ))
    .expect("compile multi-SPW problem");
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let one_row = BoundObservationSource::open(source, read_plan(4, 1), content_budget(1_024))
        .expect("bind one-row multi-SPW stream");
    let three_rows = BoundObservationSource::open(source, read_plan(4, 3), content_budget(3_072))
        .expect("bind three-row multi-SPW stream");

    let prediction = one_row
        .selected_samples(&problem)
        .expect("prepare prediction replay")
        .collect::<Result<Vec<_>, _>>()
        .expect("read prediction replay");
    let residual = one_row
        .selected_samples(&problem)
        .expect("prepare residual replay")
        .collect::<Result<Vec<_>, _>>()
        .expect("read residual replay");
    let repartitioned = three_rows
        .selected_samples(&problem)
        .expect("prepare repartitioned replay")
        .collect::<Result<Vec<_>, _>>()
        .expect("read repartitioned replay");

    assert_eq!(prediction, residual);
    assert_eq!(prediction, repartitioned);
    assert_eq!(prediction.len(), 16);
    assert_eq!(
        prediction
            .iter()
            .map(|sample| {
                (
                    sample.address.physical_row,
                    sample.address.data_description_id,
                    sample.address.spectral_window_id,
                    sample.address.channel_index,
                    sample.address.correlation_index,
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
    assert_eq!(prediction[8].address.frequency_centre_hz, 1.501e9);
    assert_eq!(prediction[12].address.frequency_centre_hz, 1.501e9);
    let prediction_inspection = problem
        .inspect_selected_observation(prediction)
        .expect("inspect prediction replay");
    let residual_inspection = problem
        .inspect_selected_observation(residual)
        .expect("inspect residual replay");
    assert_eq!(prediction_inspection, residual_inspection);
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
    let mut antennas = tutorial_vla_a_antennas();
    antennas.truncate(2);
    let mut request = SyntheticObservationRequest::vla_ppdisk("unused.fits", path, antennas);
    request.predict_model = false;
    request.allow_below_elevation_limit = true;
    request.duration_seconds = row_count as f64;
    request.integration_seconds = 1.0;
    request.spectral_setup = SyntheticSpectralSetup {
        name: "three-channel".to_string(),
        start_frequency_hz: 1.4e9,
        channel_width_hz: 1.0e6,
        channel_count: 3,
    };
    request.worker_policy = SyntheticWorkerPolicy::Fixed;
    request.row_workers = Some(1);
    request.channel_workers = Some(1);
    generate_synthetic_observation_ms(&request).expect("generate bounded disk fixture");
}

fn read_plan(row_count: usize, rows_per_block: usize) -> MsReadPlan {
    MsReadPlan::new(
        row_count,
        MsSelectionIoBudget {
            available_bytes: rows_per_block * 64,
            maximum_live_blocks: 1,
            requested_bytes_per_row: 64,
            storage_alignment_rows: None,
        },
    )
    .expect("explicit bounded row plan")
}

fn content_budget(available_bytes: usize) -> SelectedObservationContentBudget {
    content_budget_with_live_blocks(available_bytes, 1)
}

fn content_budget_with_live_blocks(
    available_bytes: usize,
    maximum_live_blocks: usize,
) -> SelectedObservationContentBudget {
    SelectedObservationContentBudget::new(available_bytes, maximum_live_blocks, 4)
}

fn compiled_problem(
    path: &std::path::Path,
    row_count: usize,
) -> casa_imaging_model::CompiledProblem {
    compiled_problem_with_sources(&[(path, 1, row_count)])
}

fn compiled_problem_with_centres(
    path: &std::path::Path,
    row_count: usize,
    centres: CentreLaws,
) -> casa_imaging_model::CompiledProblem {
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![source_input(path, 1, row_count)],
        vec![(ReferenceDataKind::Measures, identity(90))],
        ModelStateIdentity::Empty,
    ))
    .expect("compile fixed-centre observation");
    compile(ImagingRequest::new(
        specification(),
        geometry_with_centres(centres),
        ProblemInputIdentities::new(snapshot),
    ))
    .expect("compile fixed-centre problem")
}

fn compiled_problem_with_sources(
    sources: &[(&std::path::Path, u8, usize)],
) -> casa_imaging_model::CompiledProblem {
    let references = vec![(ReferenceDataKind::Measures, identity(90))];
    let sources = sources
        .iter()
        .map(|(path, source, row_count)| source_input(path, *source, *row_count))
        .collect();
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        sources,
        references,
        ModelStateIdentity::Empty,
    ))
    .expect("compile selected observation");
    compile(ImagingRequest::new(
        specification(),
        geometry(),
        ProblemInputIdentities::new(snapshot),
    ))
    .expect("compile selected-observation problem")
}

fn source_input(path: &std::path::Path, source: u8, row_count: usize) -> ObservationSourceInput {
    let selected_rows = SelectedRows::from_ordered_main_rows(
        row_count as u64,
        (0..row_count).map(|row| SelectedMainRow::new(row as u64, 0)),
    )
    .expect("selected row manifest");
    let selection = ObservationSelection::new(
        selected_rows,
        RowSelection::new(
            IdSelection::All,
            TimeSelection::All,
            UvSelection::All,
            AntennaSelection::All,
            IdSelection::All,
            IdSelection::All,
            IntentSelection::All,
            IdSelection::All,
        ),
        vec![DataDescriptionSelection::new(0, 0, 0)],
        vec![SpectralWindowSelection::new(0, vec![0, 2])],
        vec![CorrelationSelection::new(
            0,
            vec![
                CorrelationProduct::new(0, CorrelationType::CircularRr),
                CorrelationProduct::new(1, CorrelationType::CircularLl),
            ],
        )],
    );
    let columns = [
        MsColumnKind::Data,
        MsColumnKind::Flag,
        MsColumnKind::FlagRow,
        MsColumnKind::Weight,
        MsColumnKind::Uvw,
        MsColumnKind::Time,
        MsColumnKind::TimeCentroid,
        MsColumnKind::Interval,
        MsColumnKind::Exposure,
        MsColumnKind::FieldId,
        MsColumnKind::DataDescriptionId,
        MsColumnKind::Antenna1,
        MsColumnKind::Antenna2,
        MsColumnKind::Feed1,
        MsColumnKind::Feed2,
        MsColumnKind::ScanNumber,
        MsColumnKind::StateId,
        MsColumnKind::ObservationId,
        MsColumnKind::ArrayId,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, kind)| ColumnGeneration::new(kind, scoped_identity(source, 10 + index as u8)))
    .collect();
    let metadata = [
        MetadataTableKind::Antenna,
        MetadataTableKind::DataDescription,
        MetadataTableKind::Feed,
        MetadataTableKind::Field,
        MetadataTableKind::Observation,
        MetadataTableKind::Pointing,
        MetadataTableKind::Polarization,
        MetadataTableKind::SpectralWindow,
        MetadataTableKind::State,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, kind)| MetadataGeneration::new(kind, scoped_identity(source, 40 + index as u8)))
    .collect();
    ObservationSourceInput::new(
        MeasurementSetIdentity::new(scoped_identity(source, 1)),
        ObservationSourceProvenance::new(path.display().to_string(), scoped_identity(source, 2)),
        selection,
        SourceGenerations::new(
            ConsistencyToken::new(scoped_identity(source, 3)),
            SelectedColumns::new(
                VisibilityColumn::Data,
                FlagPolicy::FlagOrFlagRow,
                WeightColumn::Weight,
                columns,
            ),
            metadata,
            ModelColumnState::Absent,
        ),
    )
}

fn multi_spw_source_input(path: &std::path::Path, source: u8) -> ObservationSourceInput {
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
        RowSelection::new(
            IdSelection::All,
            TimeSelection::All,
            UvSelection::All,
            AntennaSelection::All,
            IdSelection::All,
            IdSelection::All,
            IntentSelection::All,
            IdSelection::All,
        ),
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
    let columns = [
        MsColumnKind::Data,
        MsColumnKind::Flag,
        MsColumnKind::FlagRow,
        MsColumnKind::Weight,
        MsColumnKind::Uvw,
        MsColumnKind::Time,
        MsColumnKind::TimeCentroid,
        MsColumnKind::Interval,
        MsColumnKind::Exposure,
        MsColumnKind::FieldId,
        MsColumnKind::DataDescriptionId,
        MsColumnKind::Antenna1,
        MsColumnKind::Antenna2,
        MsColumnKind::Feed1,
        MsColumnKind::Feed2,
        MsColumnKind::ScanNumber,
        MsColumnKind::StateId,
        MsColumnKind::ObservationId,
        MsColumnKind::ArrayId,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, kind)| ColumnGeneration::new(kind, scoped_identity(source, 10 + index as u8)))
    .collect();
    let metadata = [
        MetadataTableKind::Antenna,
        MetadataTableKind::DataDescription,
        MetadataTableKind::Feed,
        MetadataTableKind::Field,
        MetadataTableKind::Observation,
        MetadataTableKind::Pointing,
        MetadataTableKind::Polarization,
        MetadataTableKind::SpectralWindow,
        MetadataTableKind::State,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, kind)| MetadataGeneration::new(kind, scoped_identity(source, 40 + index as u8)))
    .collect();
    ObservationSourceInput::new(
        MeasurementSetIdentity::new(scoped_identity(source, 1)),
        ObservationSourceProvenance::new(path.display().to_string(), scoped_identity(source, 2)),
        selection,
        SourceGenerations::new(
            ConsistencyToken::new(scoped_identity(source, 3)),
            SelectedColumns::new(
                VisibilityColumn::Data,
                FlagPolicy::FlagOrFlagRow,
                WeightColumn::Weight,
                columns,
            ),
            metadata,
            ModelColumnState::Absent,
        ),
    )
}

fn identity(byte: u8) -> LogicalIdentity {
    LogicalIdentity::from_sha256([byte; 32])
}

fn scoped_identity(source: u8, byte: u8) -> LogicalIdentity {
    let mut digest = [byte; 32];
    digest[0] = source;
    LogicalIdentity::from_sha256(digest)
}

fn specification() -> ProblemSpecification {
    ProblemSpecification::new(
        ScientificContract::new(
            SpectralContract::new(SpectralSampling::Identity, SpectralCoupling::Independent),
            MeasurementEquationContract::new(
                InstrumentResponse::Scalar,
                DeclaredInnerProducts::new(
                    ModelInnerProduct::HermitianEuclidean,
                    VisibilityInnerProduct::HermitianEuclidean,
                ),
            ),
        ),
        ReconstructionContract::new(
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Hogbom,
            ReconstructionControls::new(10, 0.1, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
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
                    ProductBlankingPolicy::ZeroAndFalseMask,
                )
                .expect("valid PB policy"),
                TaylorValidityPolicy::new(
                    TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
                    0.1,
                    ProductSupportComparison::StrictlyGreater,
                    ProductBlankingPolicy::ZeroAndFalseMask,
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

fn geometry_with_centres(centres: CentreLaws) -> GeometryInput {
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
