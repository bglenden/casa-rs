// SPDX-License-Identifier: LGPL-3.0-or-later
//! Batch geometry on a small hand-made observation: the cases the
//! selected-observation route's equality law (T1, T1.5) does not reach.

use std::sync::Arc;

use casa_imaging_model::{
    BatchGeometry, BatchGeometryError, CompiledProblem, DirectionFrame, FacetLayout,
    FrequencyFrame, GeometryPlan, GeometryTraversal, GridFtProjection, PhaseCentreLaw,
    PsfPhaseCentreLaw, SkyDirection,
};
use casa_observation::{BatchBuffers, Catalogue};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{MeasFrame, MeasureError};

#[path = "fixtures/batch_observation.rs"]
mod batch_observation;
mod common;
#[path = "fixtures/model_lifecycle.rs"]
mod model_lifecycle_fixture;

use batch_observation::{
    MJD_DAYS, REFERENCE, UVW_M, catalogue, catalogue_in, fill, j2000, measures, problem,
    problem_framed,
};

/// The geometry of every sample of one time of `catalogue`'s first
/// partition under `problem`.
fn apply(problem: &CompiledProblem, catalogue: &Arc<Catalogue>) -> (GeometryPlan, BatchGeometry) {
    let plan = GeometryPlan::compile(problem, catalogue, measures()).expect("a geometry plan");
    let mut buffers = BatchBuffers::default();
    fill(&mut buffers, catalogue, 0, 1);
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
    let catalogue = catalogue(field.clone(), 1);
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
    let catalogue = catalogue(icrs.clone(), 1);
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
    let refused = GeometryPlan::compile(&problem, &catalogue(j2000(0.9, -0.45), 1), measures());
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
    let compiled = catalogue(j2000(0.9, -0.45), 1);
    let plan = GeometryPlan::compile(&problem, &compiled, measures()).expect("a geometry plan");
    let other = catalogue(j2000(0.9, -0.45), 1);
    let mut buffers = BatchBuffers::default();
    fill(&mut buffers, &other, 0, 1);
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

/// A moving phase centre batch geometry cannot follow yet (the fields' own
/// ephemerides, an ephemeris table) is refused when the plan compiles, not
/// at the first batch.
#[test]
fn a_moving_centre_measures_does_not_name_is_refused_at_compile() {
    for name in ["TRACKFIELD", "/data/mars.eph"] {
        let problem = problem(
            PhaseCentreLaw::Ephemeris(name.to_string()),
            FacetLayout::Single,
            PsfPhaseCentreLaw::ImageDomainReference,
        );
        let refused = GeometryPlan::compile(&problem, &catalogue(j2000(0.9, -0.45), 1), measures());
        assert!(
            matches!(&refused, Err(BatchGeometryError::UnsupportedEphemeris(refused)) if refused == name),
            "{name}: {refused:?}"
        );
    }
    // A body Measures names but has no model of fails as Measures does.
    let pluto = problem(
        PhaseCentreLaw::Ephemeris("PLUTO".to_string()),
        FacetLayout::Single,
        PsfPhaseCentreLaw::ImageDomainReference,
    );
    let refused = GeometryPlan::compile(&pluto, &catalogue(j2000(0.9, -0.45), 1), measures());
    assert!(
        matches!(
            refused,
            Err(BatchGeometryError::Measures(
                MeasureError::NotYetImplemented { .. }
            ))
        ),
        "{refused:?}"
    );
}

/// Rest-frame frequencies stay as they are; converting into or out of the
/// rest frame needs a source velocity, so such a batch is refused.
#[test]
fn only_rest_frequencies_stay_in_the_rest_frame() {
    let field = j2000(0.9, -0.45);
    let geometry_of = |window: FrequencyRef, source: FrequencyFrame, output: FrequencyFrame| {
        let catalogue = catalogue_in(field.clone(), 1, window);
        let problem = problem_framed(source, output);
        let plan =
            GeometryPlan::compile(&problem, &catalogue, measures()).expect("a geometry plan");
        let mut buffers = BatchBuffers::default();
        fill(&mut buffers, &catalogue, 0, 1);
        let batch = buffers.batch().expect("a batch");
        let mut geometry = BatchGeometry::default();
        plan.apply(
            &batch,
            &[true; 3],
            None,
            &mut GeometryTraversal::default(),
            &mut geometry,
        )
        .map(|()| geometry.frequencies_hz(0).to_vec())
    };
    assert_eq!(
        geometry_of(
            FrequencyRef::REST,
            FrequencyFrame::Rest,
            FrequencyFrame::Rest
        )
        .expect("rest to rest"),
        [1.4e9]
    );
    for (window, source, output) in [
        (
            FrequencyRef::REST,
            FrequencyFrame::Rest,
            FrequencyFrame::Lsrk,
        ),
        (
            FrequencyRef::TOPO,
            FrequencyFrame::Topocentric,
            FrequencyFrame::Rest,
        ),
    ] {
        let refused = geometry_of(window, source, output);
        assert!(
            matches!(refused, Err(BatchGeometryError::RestFrameWithoutVelocity)),
            "{window:?} to {output:?}: {refused:?}"
        );
    }
}

/// A time with no row samples holds no geometry: its samples' projections
/// and its frequencies stay NaN.
#[test]
fn a_time_without_rows_holds_no_geometry() {
    let problem = problem(
        PhaseCentreLaw::Observation,
        FacetLayout::Single,
        PsfPhaseCentreLaw::ImageDomainReference,
    );
    let catalogue = catalogue(j2000(0.9, -0.45), 1);
    let plan = GeometryPlan::compile(&problem, &catalogue, measures()).expect("a geometry plan");
    let mut buffers = BatchBuffers::default();
    fill(&mut buffers, &catalogue, 0, 2);
    let batch = buffers.batch().expect("a batch");
    let mut geometry = BatchGeometry::default();
    let rows = [true, true, true, false, false, false];
    plan.apply(
        &batch,
        &rows,
        None,
        &mut GeometryTraversal::default(),
        &mut geometry,
    )
    .expect("batch geometry");
    assert_eq!(geometry.frequencies_hz(0), [1.4e9]);
    assert!(geometry.frequencies_hz(1)[0].is_nan());
    for sample in 0..3 {
        assert!(geometry.projection(sample, 0).phase_shift_m.is_finite());
        assert!(geometry.projection(sample + 3, 0).phase_shift_m.is_nan());
        assert!(
            geometry
                .pointing(sample + 3)
                .antenna1
                .longitude_rad()
                .is_nan()
        );
    }
}
