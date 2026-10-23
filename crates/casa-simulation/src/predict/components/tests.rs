// SPDX-License-Identifier: LGPL-3.0-or-later
use casa_components::{
    ComponentShape, Ellipse, EllipseErrors, Flux, ShapeKind, SkyComponent, SpectralIndex,
    SpectralKind, SpectralModel,
};
use casa_types::measures::direction::MDirection;
use casa_types::measures::frequency::{FrequencyRef, MFrequency};

use super::*;
use crate::predict::PrimaryBeam;

const CENTRE: [f64; 2] = [2.6, -0.5];
const ARCSEC: f64 = std::f64::consts::PI / 180.0 / 3600.0;

fn list(components: Vec<SkyComponent>) -> ComponentList {
    let mut list = ComponentList::new();
    for component in components {
        list.push(component);
    }
    list
}

fn constant() -> SpectralModel {
    SpectralModel::new(
        MFrequency::new(230.0e9, FrequencyRef::LSRK),
        SpectralKind::Constant,
    )
}

fn point(flux: Flux, direction: [f64; 2], frame: DirectionRef) -> SkyComponent {
    SkyComponent::new(
        flux,
        ComponentShape::point(MDirection::from_angles(direction[0], direction[1], frame)),
        constant(),
    )
}

fn window(correlations: Vec<i32>) -> ComponentWindow {
    ComponentWindow {
        first_hz: 230.0e9,
        step_hz: 0.1e9,
        channels: 2,
        correlations,
    }
}

/// `list` prepared for `windows` and field 0 at [`CENTRE`].
fn prepare(
    list: &ComponentList,
    windows: &[ComponentWindow],
    beam: Option<&BeamPattern>,
) -> Result<ComponentPrediction, ComponentRefusal> {
    let targets = ComponentTargets {
        windows,
        fields: &[(0, CENTRE)],
        beam,
    };
    ComponentPrediction::new(list, targets, Reservation::none())
}

#[test]
fn a_point_at_the_phase_centre_adds_its_flux_in_the_windows_basis() {
    let components = list(vec![point(
        Flux::stokes_jy(1.0, 0.1, 0.0, 0.0),
        CENTRE,
        DirectionRef::J2000,
    )]);
    // XX and YY of a linear window: I ± Q.
    let prediction = prepare(&components, &[window(vec![9, 12])], None).expect("prepared");
    let mut out = vec![Complex32::new(0.5, 0.0); 2 * 2 * 2];
    prediction.add_rows(
        (0, 0),
        [[120.0, -40.0, 3.0], [0.0, 0.0, 0.0]].into_iter(),
        &mut out,
    );
    for (sample, value) in out.iter().enumerate() {
        let expected = if sample % 2 == 0 { 1.6 } else { 1.4 };
        assert!(
            (value.re - expected).abs() < 1.0e-6 && value.im.abs() < 1.0e-6,
            "sample {sample}: {value}"
        );
    }
}

#[test]
fn the_beam_scales_each_component_by_its_power_at_its_separation() {
    let offset = [CENTRE[0] + 1.0e-4, CENTRE[1]];
    let components = list(vec![point(
        Flux::stokes_jy(1.0, 0.0, 0.0, 0.0),
        offset,
        DirectionRef::J2000,
    )]);
    let pattern = PrimaryBeam::ALMA.pattern();
    let prediction =
        prepare(&components, &[window(vec![9, 12])], Some(&pattern)).expect("prepared");
    let mut out = vec![Complex32::new(0.0, 0.0); 2 * 2];
    // The zero spacing: the flux times the beam, whatever the phase.
    prediction.add_rows((0, 0), [[0.0, 0.0, 0.0]].into_iter(), &mut out);
    let separation = separation_rad(offset, CENTRE);
    assert!((separation - 1.0e-4 * CENTRE[1].cos()).abs() < 1.0e-12);
    let expected = pattern.power_at(separation, 230.0e9);
    assert!(expected > 0.0 && expected < 1.0);
    assert!(
        (out[0].re - expected).abs() < 1.0e-6,
        "{} against {expected}",
        out[0]
    );
}

/// A row's samples as CASA forms them, one channel at a time: each
/// component's visibility at the rotated baseline times `e^{−2πi s ν/c}`.
fn direct(
    components: &[SkyComponent],
    window: &ComponentWindow,
    basis: (Polarisation, &[usize]),
    uvw_m: [f64; 3],
) -> Vec<Complex64> {
    let mut samples = vec![Complex64::new(0.0, 0.0); window.channels * basis.1.len()];
    for component in components {
        let (ra, dec) = component.shape().reference_direction().as_angles();
        let (rotated, shift_m) =
            GridFtProjection::between_j2000_directions(CENTRE, [ra, dec]).apply(uvw_m);
        for channel in 0..window.channels {
            let frequency_hz = window.frequency_hz(channel);
            let visibility = component
                .visibility(rotated, frequency_hz, basis.0)
                .expect("visibility");
            let phase = -std::f64::consts::TAU * shift_m * frequency_hz / SPEED_OF_LIGHT_M_PER_S;
            for (correlation, &index) in basis.1.iter().enumerate() {
                samples[channel * basis.1.len() + correlation] +=
                    visibility[index] * Complex64::from_polar(1.0, phase);
            }
        }
    }
    samples
}

/// Points, a Gaussian and a disk with spectral indices, polarised, 1 to 30
/// arcsec from the centre.
fn mixed() -> Vec<SkyComponent> {
    let at = |dx: f64, dy: f64| {
        MDirection::from_angles(
            CENTRE[0] + dx * ARCSEC / CENTRE[1].cos(),
            CENTRE[1] + dy * ARCSEC,
            DirectionRef::J2000,
        )
    };
    let indexed = |alpha: f64| {
        SpectralModel::new(
            MFrequency::new(230.0e9, FrequencyRef::LSRK),
            SpectralKind::SpectralIndex(SpectralIndex::new(alpha)),
        )
    };
    let ellipse = |major: f64, minor: f64, pa_deg: f64| {
        Ellipse::new(major * ARCSEC, minor * ARCSEC, pa_deg.to_radians()).expect("axes")
    };
    vec![
        SkyComponent::new(
            Flux::stokes_jy(1.0, 0.1, -0.05, 0.02),
            ComponentShape::point(at(1.0, -2.0)),
            indexed(-0.7),
        ),
        SkyComponent::new(
            Flux::stokes_jy(0.5, 0.0, 0.0, 0.0),
            ComponentShape::new(
                at(-12.0, 7.0),
                ShapeKind::Gaussian {
                    axes: ellipse(2.0, 1.0, 30.0),
                    errors: EllipseErrors::default(),
                },
            )
            .expect("Gaussian"),
            indexed(1.5),
        ),
        SkyComponent::new(
            Flux::stokes_jy(0.3, 0.0, 0.03, 0.0),
            ComponentShape::new(
                at(20.0, 22.0),
                ShapeKind::Disk {
                    axes: ellipse(3.0, 2.0, -40.0),
                    errors: EllipseErrors::default(),
                },
            )
            .expect("disk"),
            constant(),
        ),
    ]
}

#[test]
fn a_rows_channels_predict_as_each_components_visibility_one_channel_at_a_time() {
    let components = mixed();
    let listed = list(components.clone());
    // 2,048 channels of 2 MHz: the phasor's recurrence crosses the band.
    let window = ComponentWindow {
        first_hz: 226.0e9,
        step_hz: 2.0e6,
        channels: 2_048,
        correlations: vec![5, 6, 7, 8],
    };
    let prediction = prepare(&listed, std::slice::from_ref(&window), None).expect("prepared");
    let rows = [
        [1_500.0, -2_200.0, 300.0],
        [-35.0, 12.0, -4.0],
        [8_000.0, 6_000.0, -900.0],
    ];
    let samples = window.channels * 4;
    let mut out = vec![Complex32::new(0.0, 0.0); rows.len() * samples];
    prediction.add_rows((0, 0), rows.into_iter(), &mut out);
    for (row, uvw_m) in rows.into_iter().enumerate() {
        let expected = direct(
            &components,
            &window,
            (Polarisation::Circular, &[0, 1, 2, 3]),
            uvw_m,
        );
        let worst = expected
            .iter()
            .zip(&out[row * samples..(row + 1) * samples])
            .map(|(expected, actual)| {
                (Complex64::new(f64::from(actual.re), f64::from(actual.im)) - expected).norm()
            })
            .fold(0.0, f64::max);
        // Single-precision DATA of a 1.8 Jy sum.
        assert!(worst < 1.0e-6, "row {row}: {worst} Jy from the direct sum");
    }
}

#[test]
fn a_prediction_holds_the_bytes_its_targets_say_before_it_is_formed() {
    let listed = list(mixed());
    let pattern = PrimaryBeam::ALMA.pattern();
    let windows = [window(vec![9, 12]), {
        let mut wide = window(vec![9, 10, 11, 12]);
        wide.channels = 64;
        wide
    }];
    for beam in [None, Some(&pattern)] {
        let targets = ComponentTargets {
            windows: &windows,
            fields: &[(2, CENTRE), (0, [CENTRE[0], CENTRE[1] + 1.0e-4])],
            beam,
        };
        let prediction =
            ComponentPrediction::new(&listed, targets, Reservation::none()).expect("prepared");
        assert_eq!(
            prediction.bytes(),
            targets.bytes(listed.len()),
            "beam {}",
            beam.is_some()
        );
    }
}

#[test]
fn directions_and_correlations_casa_rs_does_not_predict_are_refused() {
    let galactic = list(vec![point(
        Flux::stokes_jy(1.0, 0.0, 0.0, 0.0),
        CENTRE,
        DirectionRef::GALACTIC,
    )]);
    assert!(matches!(
        prepare(&galactic, &[window(vec![9, 12])], None),
        Err(ComponentRefusal::NotYetImplemented(reason)) if reason.contains("J2000")
    ));
    let j2000 = list(vec![point(
        Flux::stokes_jy(1.0, 0.0, 0.0, 0.0),
        CENTRE,
        DirectionRef::J2000,
    )]);
    for correlations in [vec![1], vec![5, 12]] {
        assert!(prepare(&j2000, &[window(correlations)], None).is_err());
    }
}
