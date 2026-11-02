// SPDX-License-Identifier: LGPL-3.0-or-later
//! Batch geometry of fields that follow an ephemeris (#942), on a small
//! hand-made observation: each time's field direction is the body's seen
//! from the observatory, offset by the field's phase centre; `TRACKFIELD`
//! and an ephemeris table move the phase centre with a body; and the
//! body's radial velocity takes frequencies into the rest frame.

use std::sync::Arc;

use casa_imaging_model::{
    BatchGeometry, BatchGeometryError, CompiledProblem, FacetLayout, FrequencyFrame, GeometryPlan,
    GeometryTraversal, GridFtProjection, PhaseCentreLaw, PsfPhaseCentreLaw,
};
use casa_observation::{BatchBuffers, Catalogue, FieldEphemeris};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::ephemeris::{offset_true_angle, topocentric_j2000};
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};
use casa_types::measures::{MeasFrame, PreparedFrequencyConversion};

#[path = "fixtures/batch_observation.rs"]
mod batch_observation;
mod common;
#[path = "fixtures/model_lifecycle.rs"]
mod model_lifecycle_fixture;

use batch_observation::{
    MJD_DAYS, REFERENCE, UVW_M, catalogue, catalogue_in, fill, j2000, measures, problem,
    problem_framed,
};

/// The field's phase centre: its offset from the body.
const OFFSET_RAD: [f64; 2] = [2.0e-5, -1.0e-5];

/// A body about 1 AU away near [`REFERENCE`], moving and receding, sampled
/// over a day around the observation.
fn body(name: &str, drift_rad: f64) -> Arc<FieldEphemeris> {
    const AU_M: f64 = 149_597_870_700.0;
    Arc::new(
        FieldEphemeris::new(
            name,
            vec![MJD_DAYS - 0.5, MJD_DAYS, MJD_DAYS + 0.5],
            vec![
                [REFERENCE[0], REFERENCE[1]],
                [REFERENCE[0] + drift_rad, REFERENCE[1] - drift_rad],
                [
                    REFERENCE[0] + 2.0 * drift_rad,
                    REFERENCE[1] - 2.0 * drift_rad,
                ],
            ],
            DirectionRef::ICRS,
            vec![AU_M, 1.001 * AU_M, 1.002 * AU_M],
            (vec![15_000.0, 16_000.0, 17_000.0], RadialVelocityRef::GEO),
        )
        .expect("an ephemeris"),
    )
}

/// `catalogue`'s field following `ephemeris`, its phase centre the offset.
fn following(catalogue: &Catalogue, ephemeris: &Arc<FieldEphemeris>) -> Arc<Catalogue> {
    let mut moving = catalogue.clone();
    moving.fields[0].phase_centre =
        MDirection::from_angles(OFFSET_RAD[0], OFFSET_RAD[1], DirectionRef::J2000);
    moving.fields[0].ephemeris = Some(Arc::clone(ephemeris));
    Arc::new(moving)
}

/// The observatory's frame at `time`, a time of the batch, on UTC: the
/// fixture's observatory is its first antenna.
fn frame_at(catalogue: &Catalogue, time: usize) -> MeasFrame {
    MeasFrame::new()
        .with_epoch(MEpoch::from_mjd_seconds(
            MJD_DAYS * 86_400.0 + 10.0 * time as f64,
            EpochRef::UTC,
        ))
        .with_position(catalogue.antennas[0].position.clone())
        .with_measures(measures())
}

/// `ephemeris` at `frame`'s epoch on `scale`.
fn sample_at(
    ephemeris: &FieldEphemeris,
    frame: &MeasFrame,
    scale: EpochRef,
) -> casa_observation::EphemerisSample {
    let mjd = frame
        .epoch()
        .expect("an epoch")
        .convert_to(scale, frame)
        .expect("a time scale")
        .value()
        .as_mjd();
    ephemeris.sample(mjd).expect("inside the ephemeris")
}

/// The body's J2000 direction from the observatory at `time`, interpolated
/// at the time's TDB.
fn body_direction(catalogue: &Catalogue, ephemeris: &FieldEphemeris, time: usize) -> MDirection {
    let frame = frame_at(catalogue, time);
    topocentric_j2000(
        sample_at(ephemeris, &frame, EpochRef::TDB).position_m,
        ephemeris.frame(),
        &catalogue.antennas[0].position,
        &frame,
    )
    .expect("a topocentric direction")
}

/// The geometry of every sample of `times` times of `catalogue`'s first
/// partition under `problem`, its moving centre following `table` when one
/// is given.
fn apply(
    problem: &CompiledProblem,
    catalogue: &Arc<Catalogue>,
    table: Option<Arc<FieldEphemeris>>,
    times: usize,
) -> Result<BatchGeometry, BatchGeometryError> {
    let plan = GeometryPlan::compile(problem, catalogue, measures(), table)?;
    let mut buffers = BatchBuffers::default();
    fill(&mut buffers, catalogue, 0, times);
    let batch = buffers.batch().expect("a batch");
    let mut geometry = BatchGeometry::default();
    plan.apply(
        &batch,
        &vec![true; 3 * times],
        None,
        &mut GeometryTraversal::default(),
        &mut geometry,
    )?;
    Ok(geometry)
}

/// Every sample of `time` reprojected by `machine`, to rounding.
fn assert_projected(geometry: &BatchGeometry, time: usize, machine: &GridFtProjection) {
    for (baseline, uvw) in UVW_M.iter().enumerate() {
        let sample = 3 * time + baseline;
        let projection = geometry.projection(sample, 0);
        let (uvw_m, phase_shift_m) = machine.apply(*uvw);
        let difference = (0..3)
            .map(|axis| (projection.uvw_m[axis] - uvw_m[axis]).abs())
            .fold((projection.phase_shift_m - phase_shift_m).abs(), f64::max);
        assert!(
            difference <= 1.0e-9,
            "time {time}, baseline {baseline}: {projection:?} against {uvw_m:?}, {phase_shift_m}"
        );
    }
}

/// A field that follows an ephemeris is observed, at each time, towards the
/// body's direction from the observatory shifted by its phase centre's true
/// angle, as CASA's imager evaluates it: rows reproject from there to a
/// fixed image centre, time by time.
#[test]
fn a_moving_field_is_observed_towards_the_body_offset_by_its_phase_centre() {
    let comet = body("comet", 1.0e-4);
    let catalogue = following(&catalogue(j2000(0.9, -0.45), 1), &comet);
    let fixed = problem(
        PhaseCentreLaw::Fixed(casa_imaging_model::SkyDirection::new(
            casa_imaging_model::DirectionFrame::J2000,
            REFERENCE[0],
            REFERENCE[1],
        )),
        FacetLayout::Single,
        PsfPhaseCentreLaw::ImageDomainReference,
    );
    let geometry = apply(&fixed, &catalogue, None, 2).expect("batch geometry");
    let mut observed = Vec::new();
    for time in 0..2 {
        let direction = offset_true_angle(&body_direction(&catalogue, &comet, time), OFFSET_RAD)
            .expect("the offset direction");
        assert_projected(
            &geometry,
            time,
            &GridFtProjection::between_directions(&direction, &j2000(REFERENCE[0], REFERENCE[1])),
        );
        observed.push(direction.as_angles());
    }
    assert_ne!(observed[0], observed[1], "the field moves between times");
}

/// `TRACKFIELD` moves the phase centre with each field's own body, without
/// the field's offset; the image follows it from its reference direction,
/// so rows reproject from the field's direction to the body's.
#[test]
fn trackfield_follows_each_fields_own_body() {
    let comet = body("comet", 1.0e-4);
    let catalogue = following(&catalogue(j2000(0.9, -0.45), 1), &comet);
    let tracking = problem(
        PhaseCentreLaw::Ephemeris("TRACKFIELD".to_string()),
        FacetLayout::Single,
        PsfPhaseCentreLaw::ImageDomainReference,
    );
    let geometry = apply(&tracking, &catalogue, None, 2).expect("batch geometry");
    for time in 0..2 {
        let body = body_direction(&catalogue, &comet, time);
        let field = offset_true_angle(&body, OFFSET_RAD).expect("the offset direction");
        assert_projected(
            &geometry,
            time,
            &GridFtProjection::between_directions(&field, &body),
        );
    }
}

/// An ephemeris table's body moves the phase centre for a fixed field; the
/// table, not Measures, gives the body named by the law.
#[test]
fn an_ephemeris_table_moves_the_phase_centre_with_its_body() {
    let field = j2000(0.9, -0.45);
    let catalogue = catalogue(field.clone(), 1);
    let table = body("comet", 1.0e-4);
    let following_table = problem(
        PhaseCentreLaw::Ephemeris("/data/comet.tab".to_string()),
        FacetLayout::Single,
        PsfPhaseCentreLaw::ImageDomainReference,
    );
    let geometry =
        apply(&following_table, &catalogue, Some(Arc::clone(&table)), 1).expect("batch geometry");
    assert_projected(
        &geometry,
        0,
        &GridFtProjection::between_directions(&field, &body_direction(&catalogue, &table, 0)),
    );
}

/// Under `TRACKFIELD`, frequencies convert into the rest frame with the
/// field's body's radial velocity, interpolated at the time's UTC, towards
/// the field's direction at the time.
#[test]
fn rest_frequencies_take_the_bodys_radial_velocity() {
    let comet = body("comet", 1.0e-4);
    let catalogue = following(
        &catalogue_in(j2000(0.9, -0.45), 1, FrequencyRef::TOPO),
        &comet,
    );
    let rest = problem_framed(
        PhaseCentreLaw::Ephemeris("TRACKFIELD".to_string()),
        FrequencyFrame::Topocentric,
        FrequencyFrame::Rest,
    );
    let geometry = apply(&rest, &catalogue, None, 1).expect("batch geometry");
    let frame = frame_at(&catalogue, 0);
    let velocity = sample_at(&comet, &frame, EpochRef::UTC).radial_velocity_m_s;
    let direction = offset_true_angle(&body_direction(&catalogue, &comet, 0), OFFSET_RAD)
        .expect("the offset direction");
    let frame = frame
        .with_direction(direction)
        .with_radial_velocity(MRadialVelocity::new(velocity, RadialVelocityRef::GEO));
    let expected = PreparedFrequencyConversion::new(
        FrequencyRef::TOPO,
        FrequencyRef::REST,
        Some(&frame),
        Some(&frame),
    )
    .expect("a rest-frame conversion")
    .convert_hz(1.4e9);
    assert_eq!(geometry.frequencies_hz(0), [expected]);
    assert!(
        (expected / 1.4e9 - 1.0).abs() > 1.0e-5,
        "the velocity shifts the frequency: {expected}"
    );
}

/// What a moving field or centre cannot be evaluated with is refused:
/// `TRACKFIELD` over a field that follows no ephemeris (at the batch), a
/// table for a centre that does not move (at compile), and a time outside
/// the ephemeris's samples.
#[test]
fn what_an_ephemeris_cannot_answer_is_refused() {
    let tracking = problem(
        PhaseCentreLaw::Ephemeris("TRACKFIELD".to_string()),
        FacetLayout::Single,
        PsfPhaseCentreLaw::ImageDomainReference,
    );
    let fixed_field = catalogue(j2000(0.9, -0.45), 1);
    assert!(matches!(
        apply(&tracking, &fixed_field, None, 1),
        Err(BatchGeometryError::FieldWithoutEphemeris(field)) if field.0 == 0
    ));
    let observation = problem(
        PhaseCentreLaw::Observation,
        FacetLayout::Single,
        PsfPhaseCentreLaw::ImageDomainReference,
    );
    assert!(matches!(
        apply(&observation, &fixed_field, Some(body("comet", 0.0)), 1),
        Err(BatchGeometryError::UnusedEphemerisTable)
    ));
    let early = Arc::new(
        FieldEphemeris::new(
            "early",
            vec![MJD_DAYS - 2.0, MJD_DAYS - 1.0],
            vec![REFERENCE; 2],
            DirectionRef::ICRS,
            vec![1.0e11; 2],
            (vec![0.0; 2], RadialVelocityRef::GEO),
        )
        .expect("an ephemeris"),
    );
    let refused = apply(&observation, &following(&fixed_field, &early), None, 1);
    assert!(
        matches!(&refused, Err(BatchGeometryError::EphemerisCoverage { body, .. }) if body == "early"),
        "{refused:?}"
    );
}
