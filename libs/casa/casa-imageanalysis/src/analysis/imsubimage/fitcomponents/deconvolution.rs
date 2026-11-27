// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::PI;

use casa_images::GaussianBeam;

use super::uncertainty::Uncertainty;

pub(super) struct Deconvolved {
    pub(super) gaussian: Option<(GaussianBeam, Uncertainty)>,
    pub(super) is_point: bool,
}

pub(super) fn deconvolve(
    convolved: GaussianBeam,
    beam: GaussianBeam,
    errors: &Uncertainty,
) -> Deconvolved {
    match convolved.fitted_deconvolution(beam) {
        None => Deconvolved {
            gaussian: None,
            is_point: true,
        },
        Some((_, true)) => Deconvolved {
            gaussian: upper_limit(convolved, beam, errors),
            is_point: true,
        },
        Some((source, false)) => Deconvolved {
            gaussian: Some((source, deconvolved_errors(source, convolved, beam, errors))),
            is_point: false,
        },
    }
}

fn constrained_axes(major: f64, minor: f64, angle: f64) -> GaussianBeam {
    GaussianBeam::new(major.max(minor), major.min(minor), angle)
}

fn upper_limit(
    convolved: GaussianBeam,
    beam: GaussianBeam,
    errors: &Uncertainty,
) -> Option<(GaussianBeam, Uncertainty)> {
    let major = convolved.major + errors.major;
    let minor = convolved.minor + errors.minor;
    let largest = [-1., 1.]
        .into_iter()
        .filter_map(|sign| {
            constrained_axes(major, minor, convolved.position_angle + sign * errors.angle)
                .fitted_deconvolution(beam)
                .and_then(|(shape, point)| (!point).then_some(shape))
        })
        .reduce(|a, b| GaussianBeam::new(a.major.max(b.major), a.minor.max(b.minor), 0.))?;
    let tiny = 1e-60 * PI / (180. * 3600.);
    Some((
        GaussianBeam::new(tiny, tiny, 0.),
        Uncertainty {
            major: largest.major,
            minor: largest.minor,
            angle: 0.,
            ..*errors
        },
    ))
}

fn deconvolved_errors(
    source: GaussianBeam,
    convolved: GaussianBeam,
    beam: GaussianBeam,
    errors: &Uncertainty,
) -> Uncertainty {
    let mut result = *errors;
    let centers = [convolved.major, convolved.minor, convolved.position_angle];
    let ranges = [errors.major, errors.minor, errors.angle];
    for directions in 0_u8..8 {
        let [major, minor, angle] = std::array::from_fn(|i| {
            let sign = if directions & (1 << i) != 0 { 1. } else { -1. };
            centers[i] + sign * ranges[i]
        });
        if major <= 0. || minor <= 0. {
            continue;
        }
        if let Some((candidate, false)) =
            constrained_axes(major, minor, angle).fitted_deconvolution(beam)
        {
            let angle_error = (candidate.position_angle - source.position_angle).rem_euclid(PI);
            result.major = result.major.max((source.major - candidate.major).abs());
            result.minor = result.minor.max((source.minor - candidate.minor).abs());
            result.angle = result.angle.max(angle_error.min(PI - angle_error));
        }
    }
    result
}
