// SPDX-License-Identifier: LGPL-3.0-or-later

//! Unit-consistent algebra for two-dimensional elliptical Gaussians.
//!
//! Axes may use any common angular unit. Position angles use radians in the
//! CASA convention: east from north, modulo pi.

use std::{error::Error, fmt};

/// One elliptical Gaussian described by full-width-at-half-maximum axes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EllipticalGaussian {
    /// Major-axis FWHM in a caller-chosen angular unit.
    pub major: f64,
    /// Minor-axis FWHM in the same unit as [`Self::major`].
    pub minor: f64,
    /// CASA position angle, east from north, in radians.
    pub position_angle: f64,
}

impl EllipticalGaussian {
    /// Construct one numerical Gaussian descriptor.
    #[must_use]
    pub const fn new(major: f64, minor: f64, position_angle: f64) -> Self {
        Self {
            major,
            minor,
            position_angle,
        }
    }

    /// Return the Gaussian area in squared axis units.
    #[must_use]
    pub fn area(self) -> f64 {
        std::f64::consts::PI / (4.0 * 2.0_f64.ln()) * self.major * self.minor
    }
}

/// Failure of elliptical-Gaussian enclosure or deconvolution algebra.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EllipticalGaussianError {
    /// No usable Gaussian was supplied.
    EmptySet,
    /// A Gaussian had non-finite, non-positive, or inverted axes.
    InvalidGaussian,
    /// The requested target cannot be produced by convolving the source.
    TargetSmallerThanSource,
}

impl fmt::Display for EllipticalGaussianError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptySet => "elliptical Gaussian set is empty",
            Self::InvalidGaussian => {
                "elliptical Gaussian requires finite positive major >= minor axes"
            }
            Self::TargetSmallerThanSource => "target Gaussian is smaller than the source Gaussian",
        })
    }
}

impl Error for EllipticalGaussianError {}

/// Wrap a CASA beam position angle into `(-pi/2, pi/2]`.
#[must_use]
pub fn normalize_position_angle(angle: f64) -> f64 {
    // GaussianBeam::_unwrap returns an already canonical angle unchanged.
    if angle > -std::f64::consts::FRAC_PI_2 && angle <= std::f64::consts::FRAC_PI_2 {
        return angle;
    }
    let mut wrapped = normalize_angle_pi(angle);
    if wrapped <= -std::f64::consts::FRAC_PI_2 {
        wrapped += std::f64::consts::PI;
    } else if wrapped > std::f64::consts::FRAC_PI_2 {
        wrapped -= std::f64::consts::PI;
    }
    wrapped
}

/// Compute CASA's minimum-area common enclosing Gaussian.
///
/// This is the numerical core of `CasaImageBeamSet::getCommonBeam()`.
/// `radians_per_axis_unit` converts each FWHM axis to radians; the returned
/// axes use the caller's unit. CASA constructs each enclosing ellipse in
/// arcseconds and tests it in radians.
///
/// # Errors
///
/// Returns [`EllipticalGaussianError::EmptySet`] for no beams and
/// [`EllipticalGaussianError::InvalidGaussian`] for invalid beam parameters.
pub fn common_enclosing_gaussian(
    gaussians: &[EllipticalGaussian],
    radians_per_axis_unit: f64,
) -> Result<EllipticalGaussian, EllipticalGaussianError> {
    if gaussians.is_empty() {
        return Err(EllipticalGaussianError::EmptySet);
    }
    if !radians_per_axis_unit.is_finite()
        || radians_per_axis_unit <= 0.0
        || gaussians.iter().any(|gaussian| !valid(*gaussian))
    {
        return Err(EllipticalGaussianError::InvalidGaussian);
    }
    if gaussians.iter().all(|gaussian| *gaussian == gaussians[0]) {
        return Ok(gaussians[0]);
    }
    let mut radians = gaussians.to_vec();
    if radians_per_axis_unit != 1.0 {
        for beam in &mut radians {
            beam.major *= radians_per_axis_unit;
            beam.minor *= radians_per_axis_unit;
        }
    }
    let mut common = common_enclosing_iterative(&mut radians)?;
    if radians_per_axis_unit != 1.0 {
        common.major /= radians_per_axis_unit;
        common.minor /= radians_per_axis_unit;
    }
    Ok(common)
}

/// Compute the Gaussian that convolves `source` into `target`.
///
/// Returns `Ok(None)` when target and source are effectively identical.
///
/// # Errors
///
/// Returns [`EllipticalGaussianError::TargetSmallerThanSource`] when the
/// covariance difference is not positive semidefinite.
pub fn deconvolving_gaussian(
    target: EllipticalGaussian,
    source: EllipticalGaussian,
) -> Result<Option<EllipticalGaussian>, EllipticalGaussianError> {
    if !valid(target) || !valid(source) {
        return Err(EllipticalGaussianError::InvalidGaussian);
    }
    let lhs = gaussian_covariance(target);
    let rhs = gaussian_covariance(source);
    let delta = [
        [lhs[0][0] - rhs[0][0], lhs[0][1] - rhs[0][1]],
        [lhs[1][0] - rhs[1][0], lhs[1][1] - rhs[1][1]],
    ];
    let trace = delta[0][0] + delta[1][1];
    let determinant = delta[0][0] * delta[1][1] - delta[0][1] * delta[1][0];
    let discriminant = ((trace * trace) / 4.0 - determinant).max(0.0).sqrt();
    let lambda_major = trace / 2.0 + discriminant;
    let lambda_minor = trace / 2.0 - discriminant;

    // These are the existing CASA-compatible absolute covariance tolerances
    // for radian beam axes: one treats numerical zero as a point source and
    // the other rejects a genuinely negative covariance eigenvalue.
    if lambda_minor < -1.0e-12 || lambda_major < -1.0e-12 {
        return Err(EllipticalGaussianError::TargetSmallerThanSource);
    }
    if lambda_major <= 1.0e-24 && lambda_minor <= 1.0e-24 {
        return Ok(None);
    }
    let x_axis_angle =
        if delta[0][1].abs() <= 1.0e-18 && (delta[0][0] - delta[1][1]).abs() <= 1.0e-18 {
            0.0
        } else {
            0.5 * (2.0 * delta[0][1]).atan2(delta[0][0] - delta[1][1])
        };
    Ok(Some(from_x_axis(
        lambda_major.max(0.0).sqrt(),
        lambda_minor.max(0.0).sqrt(),
        x_axis_angle,
    )))
}

fn valid(gaussian: EllipticalGaussian) -> bool {
    gaussian.major.is_finite()
        && gaussian.minor.is_finite()
        && gaussian.position_angle.is_finite()
        && gaussian.major > 0.0
        && gaussian.minor > 0.0
        && gaussian.major >= gaussian.minor
}

/// `ImageBeamSet` keeps the first beam when two have the same area.
fn largest_area(gaussians: &[EllipticalGaussian]) -> Option<(usize, &EllipticalGaussian)> {
    gaussians
        .iter()
        .enumerate()
        .max_by(|(left_index, lhs), (right_index, rhs)| {
            lhs.area()
                .total_cmp(&rhs.area())
                .then_with(|| right_index.cmp(left_index))
        })
}

fn common_enclosing_iterative(
    gaussians: &mut [EllipticalGaussian],
) -> Result<EllipticalGaussian, EllipticalGaussianError> {
    loop {
        let (max_index, &max_gaussian) =
            largest_area(gaussians).ok_or(EllipticalGaussianError::EmptySet)?;

        // CASA skips every beam equal to the largest (`*iBeam != maxBeam`),
        // not only the largest itself: a duplicate (Stokes planes share a
        // beam) can fail the enclosure test by rounding, and enclosing a
        // beam with itself gives no ellipse.
        let problem = gaussians
            .iter()
            .copied()
            .enumerate()
            .filter(|(index, gaussian)| *index != max_index && *gaussian != max_gaussian)
            .map(|(_, gaussian)| gaussian)
            .rfind(|gaussian| !encloses(max_gaussian, *gaussian));
        let Some(problem) = problem else {
            return Ok(max_gaussian);
        };

        let relative_angle = normalize_position_angle(problem.position_angle)
            - normalize_position_angle(max_gaussian.position_angle);
        if (normalize_angle_pi(relative_angle).abs() - std::f64::consts::FRAC_PI_2).abs() <= 1.0e-12
        {
            let max_has_major = max_gaussian.major >= problem.major;
            return Ok(EllipticalGaussian::new(
                if max_has_major {
                    max_gaussian.major
                } else {
                    problem.major
                },
                if max_has_major {
                    problem.major
                } else {
                    max_gaussian.major
                },
                normalize_position_angle(if max_has_major {
                    max_gaussian.position_angle
                } else {
                    problem.position_angle
                }),
            ));
        }

        // CASA constructs the enclosing ellipse in arcseconds before its
        // Gaussian deconvolution checks convert those axes back to radians.
        let arcseconds_per_radian = 180.0 * 3_600.0 / std::f64::consts::PI;
        let radians_per_arcsecond = std::f64::consts::PI / (180.0 * 3_600.0);
        let max_major = max_gaussian.major * arcseconds_per_radian;
        let max_minor = max_gaussian.minor * arcseconds_per_radian;
        let equal_area_axis = (max_major * max_minor).sqrt();
        let x_scale = equal_area_axis / max_major;
        let y_scale = equal_area_axis / max_minor;
        let (problem_major, _, problem_angle) = transform_ellipse_by_scaling(
            problem.major * arcseconds_per_radian,
            problem.minor * arcseconds_per_radian,
            relative_angle,
            x_scale,
            y_scale,
        );
        let (mut major, mut minor, common_angle) = transform_ellipse_by_scaling(
            problem_major,
            equal_area_axis,
            problem_angle,
            1.0 / x_scale,
            1.0 / y_scale,
        );
        let position_angle = common_angle + normalize_position_angle(max_gaussian.position_angle);
        // A degenerate construction would never enclose the pair: refuse it
        // rather than grow it for ever.
        if !(major.is_finite() && minor.is_finite() && position_angle.is_finite())
            || major <= 0.0
            || minor <= 0.0
        {
            return Err(EllipticalGaussianError::InvalidGaussian);
        }
        let gaussian = |major, minor| {
            EllipticalGaussian::new(
                major * radians_per_arcsecond,
                minor * radians_per_arcsecond,
                normalize_position_angle(position_angle),
            )
        };
        let mut enclosing = gaussian(major, minor);
        while !(encloses(enclosing, max_gaussian) && encloses(enclosing, problem)) {
            major *= 1.001;
            minor *= 1.001;
            enclosing = gaussian(major, minor);
        }

        gaussians[max_index] = enclosing;
    }
}

fn from_x_axis(major: f64, minor: f64, x_axis_angle: f64) -> EllipticalGaussian {
    EllipticalGaussian::new(
        major,
        minor,
        normalize_position_angle(x_axis_angle - std::f64::consts::FRAC_PI_2),
    )
}

fn normalize_angle_pi(angle: f64) -> f64 {
    let mut wrapped = angle.rem_euclid(2.0 * std::f64::consts::PI);
    if wrapped > std::f64::consts::PI {
        wrapped -= 2.0 * std::f64::consts::PI;
    }
    wrapped
}

fn encloses(enclosing: EllipticalGaussian, other: EllipticalGaussian) -> bool {
    let theta_source = normalize_position_angle(enclosing.position_angle);
    let theta_beam = normalize_position_angle(other.position_angle);
    let alpha = (enclosing.major * theta_source.cos()).powi(2)
        + (enclosing.minor * theta_source.sin()).powi(2)
        - (other.major * theta_beam.cos()).powi(2)
        - (other.minor * theta_beam.sin()).powi(2);
    let beta = (enclosing.major * theta_source.sin()).powi(2)
        + (enclosing.minor * theta_source.cos()).powi(2)
        - (other.major * theta_beam.sin()).powi(2)
        - (other.minor * theta_beam.cos()).powi(2);
    let gamma = 2.0
        * (((enclosing.minor * enclosing.minor) - (enclosing.major * enclosing.major))
            * theta_source.sin()
            * theta_source.cos()
            - ((other.minor * other.minor) - (other.major * other.major))
                * theta_beam.sin()
                * theta_beam.cos());
    let sum = alpha + beta;
    let difference = ((alpha - beta).powi(2) + gamma.powi(2)).sqrt();
    alpha >= 0.0 && beta >= 0.0 && sum >= difference
}

/// Return the symmetric FWHM-squared covariance form in image x/y axes.
///
/// The common Gaussian-to-standard-deviation factor is deliberately omitted:
/// it cancels in enclosure and deconvolution and preserves CASA arithmetic.
#[must_use]
pub fn gaussian_covariance(gaussian: EllipticalGaussian) -> [[f64; 2]; 2] {
    let angle = normalize_position_angle(gaussian.position_angle) + std::f64::consts::FRAC_PI_2;
    let cos = angle.cos();
    let sin = angle.sin();
    let major2 = gaussian.major * gaussian.major;
    let minor2 = gaussian.minor * gaussian.minor;
    [
        [
            cos * cos * major2 + sin * sin * minor2,
            cos * sin * (major2 - minor2),
        ],
        [
            cos * sin * (major2 - minor2),
            sin * sin * major2 + cos * cos * minor2,
        ],
    ]
}

fn transform_ellipse_by_scaling(
    major: f64,
    minor: f64,
    position_angle: f64,
    x_scale: f64,
    y_scale: f64,
) -> (f64, f64, f64) {
    let cos = position_angle.cos();
    let sin = position_angle.sin();
    let cos2 = cos * cos;
    let sin2 = sin * sin;
    let major2 = major * major;
    let minor2 = minor * minor;
    let a = cos2 / major2 + sin2 / minor2;
    let b = -2.0 * cos * sin * (1.0 / major2 - 1.0 / minor2);
    let c = sin2 / major2 + cos2 / minor2;
    let x_scale2 = x_scale * x_scale;
    let y_scale2 = y_scale * y_scale;
    let r = a / x_scale2;
    let s = b * b / (4.0 * x_scale2 * y_scale2);
    let t = c / y_scale2;
    let difference = r - t;
    let discriminant = difference * difference + 4.0 * s;
    let signed = discriminant.sqrt() * difference.abs();
    let j1 = (signed + discriminant) / discriminant / 2.0;
    let j2 = (-signed + discriminant) / discriminant / 2.0;
    let k1 = (j1 * r + j1 * t - t) / (2.0 * j1 - 1.0);
    let k2 = (j2 * r + j2 * t - t) / (2.0 * j2 - 1.0);
    let axis1 = (1.0 / k1).sqrt();
    let axis2 = (1.0 / k2).sqrt();

    // CASA's `_transformEllipseByScaling` takes the ellipse for a circle
    // only when its axes are exactly equal, whatever their unit. (Its circle
    // has the reciprocal of the axis, `sqrt(k1)`, against its evident intent;
    // the circle has the axis.)
    if axis1 == axis2 {
        return (axis1, axis1, 0.0);
    }
    if axis1 > axis2 {
        (
            axis1,
            axis2,
            if position_angle >= 0.0 {
                j1.sqrt().acos()
            } else {
                -j1.sqrt().acos()
            },
        )
    } else {
        (
            axis2,
            axis1,
            if position_angle >= 0.0 {
                j2.sqrt().acos()
            } else {
                -j2.sqrt().acos()
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Casacore's `minMax` keeps the first maximum, even when the equal-area
    /// beams differ in shape and can produce different enclosing ellipses.
    #[test]
    fn the_first_equal_area_beam_is_the_maximum() {
        let beams = [
            EllipticalGaussian::new(4.0, 2.0, 0.0),
            EllipticalGaussian::new(8.0, 1.0, 0.0),
        ];
        assert_eq!(largest_area(&beams).map(|(index, _)| index), Some(0));
    }

    /// GaussianBeam::_unwrap leaves an angle inside its canonical interval
    /// unchanged, avoiding a lossy round trip through a full turn.
    #[test]
    fn canonical_position_angles_are_left_unchanged() {
        let angle = -0.349_065_850_398_865_9;
        assert_eq!(normalize_position_angle(angle), angle);
    }

    #[test]
    fn common_envelope_reuses_one_working_set_for_many_planes() {
        let pair = [
            EllipticalGaussian::new(7.0, 4.0, 35_f64.to_radians()),
            EllipticalGaussian::new(6.0, 5.0, -20_f64.to_radians()),
        ];
        let expected = common_enclosing_gaussian(&pair, 1.0).unwrap();
        let repeated = pair.into_iter().cycle().take(16_384).collect::<Vec<_>>();
        assert_eq!(common_enclosing_gaussian(&repeated, 1.0).unwrap(), expected);
    }

    /// CASA 6.7.6.14 `image.commonbeam()` for a 10-channel, two-Stokes
    /// image with a blank PSF replaced by the image's largest beam. CASA's
    /// beam matrix visits channels first within each polarization.
    #[test]
    fn common_envelope_matches_casas_multistokes_beam_matrix() {
        let values = [
            (
                0.000_659_920_363_505_466_7,
                0.000_277_714_375_479_345_24,
                -1.006_629_886_598_266_8,
            ),
            (
                0.000_489_199_949_523_298_2,
                0.000_332_711_701_991_262_23,
                0.534_970_100_282_602_5,
            ),
            (
                0.000_575_668_982_898_358_4,
                0.000_251_585_111_889_876_9,
                0.498_537_038_391_696_7,
            ),
            (
                0.000_445_553_375_482_662,
                0.000_232_902_929_545_132_06,
                0.677_320_743_101_792_8,
            ),
        ];
        let beams = values.map(|(major, minor, pa)| EllipticalGaussian::new(major, minor, pa));
        let mut channel_major = vec![beams[0], beams[1], beams[2], beams[3]];
        channel_major.extend([beams[3]; 16]);
        let matrix = (0..2)
            .flat_map(|pol| (0..10).map(move |channel| channel * 2 + pol))
            .map(|index| channel_major[index])
            .collect::<Vec<_>>();
        let common = common_enclosing_gaussian(&matrix, 1.0).expect("a common beam");
        let arcsec = std::f64::consts::PI / (180.0 * 3_600.0);
        assert!(
            (common.major / arcsec - 137.588_692_242_4).abs() < 1.0e-4,
            "{common:?}"
        );
        assert!(
            (common.minor / arcsec - 118.268_296_464_4).abs() < 1.0e-4,
            "{common:?}"
        );
        assert!(
            (common.position_angle.to_degrees() + 49.863_390_056_5).abs() < 1.0e-4,
            "{common:?}"
        );
    }

    /// CASA's arcsecond intermediate changes the selected enclosing beam by
    /// more than the beam-width tolerance for this four-ellipse case.
    #[test]
    fn common_envelope_uses_casas_arcsecond_construction() {
        let beams = [
            EllipticalGaussian::new(
                0.000_635_663_281_774_671_6,
                0.000_221_068_499_697_525_18,
                0.698_202_265_589_554_6,
            ),
            EllipticalGaussian::new(
                0.000_331_624_008_949_999_7,
                0.000_132_309_620_683_129_18,
                0.009_212_392_884_049_958,
            ),
            EllipticalGaussian::new(
                0.000_545_372_831_442_117_8,
                0.000_484_014_204_578_454_9,
                0.762_798_083_901_558_7,
            ),
            EllipticalGaussian::new(
                0.000_749_307_589_950_919_8,
                0.000_369_864_343_484_647_7,
                -0.415_377_347_404_582_76,
            ),
        ];
        let actual = common_enclosing_gaussian(&beams, 1.0).expect("common beam");
        let expected = [
            0.000_765_088_472_784_056_5,
            0.000_588_291_796_685_771_8,
            -0.222_854_456_601_728_03,
        ];
        assert!(
            (actual.major / expected[0] - 1.0).abs() < 1.0e-5,
            "{actual:?}"
        );
        assert!(
            (actual.minor / expected[1] - 1.0).abs() < 1.0e-5,
            "{actual:?}"
        );
        assert!(
            (actual.position_angle - expected[2]).abs() < 1.0e-5,
            "{actual:?}"
        );
        let arcsec = std::f64::consts::PI / (180.0 * 3_600.0);
        let arcsecond_beams = beams.map(|beam| {
            EllipticalGaussian::new(
                beam.major / arcsec,
                beam.minor / arcsec,
                beam.position_angle,
            )
        });
        let converted = common_enclosing_gaussian(&arcsecond_beams, arcsec).unwrap();
        assert!(
            (converted.major * arcsec / expected[0] - 1.0).abs() < 1.0e-5,
            "{converted:?}"
        );
        assert!(
            (converted.minor * arcsec / expected[1] - 1.0).abs() < 1.0e-5,
            "{converted:?}"
        );
        assert!(
            (converted.position_angle - expected[2]).abs() < 1.0e-5,
            "{converted:?}"
        );
    }

    #[test]
    fn common_envelope_matches_the_casa_two_beam_reference() {
        let arcsec = std::f64::consts::PI / (180.0 * 3_600.0);
        let beams = [
            EllipticalGaussian::new(7.0 * arcsec, 4.0 * arcsec, 35_f64.to_radians()),
            EllipticalGaussian::new(6.0 * arcsec, 5.0 * arcsec, -20_f64.to_radians()),
        ];
        let common = common_enclosing_gaussian(&beams, 1.0).expect("common beam");
        // CASA 6.7.6.14 `image.commonbeam()` on this exact two-plane set.
        assert!(
            (common.major / arcsec - 7.116_149_425_836_256).abs() < 1.0e-6,
            "computed common beam: {common:?}"
        );
        assert!(
            (common.minor / arcsec - 5.640_938_804_984_346).abs() < 1.0e-6,
            "computed common beam: {common:?}"
        );
        assert!(
            (common.position_angle.to_degrees() - 23.593_728_922_728_804).abs() < 1.0e-6,
            "computed common beam: {common:?}"
        );
    }

    /// A beam equal to the largest is passed over, as CASA passes it over:
    /// two Stokes planes' equal 0.09 by 0.05 arcsec beams (radians) failed
    /// the enclosure test by rounding, and enclosing the largest with its
    /// own copy gave no ellipse.
    #[test]
    fn a_copy_of_the_largest_beam_is_passed_over() {
        let largest = EllipticalGaussian::new(
            4.471_597_567_379_012e-7,
            2.275_313_022_046_005_3e-7,
            -0.006_651_467_446_095_271,
        );
        let smaller = EllipticalGaussian::new(
            4.459_097_676_174_015e-7,
            2.269_180_481_074_049_3e-7,
            -0.006_636_139_185_516_454,
        );
        assert_eq!(
            common_enclosing_gaussian(&[largest, largest, smaller, smaller], 1.0),
            Ok(largest)
        );
    }

    /// A scaled ellipse that is nearly, not exactly, a circle keeps its
    /// axes: an absolute tolerance of 1e-12 took sub-arcsecond beams in
    /// radians (a few 1e-7) within 1e-5 of circular for a circle of the
    /// reciprocal axis, about 3e6, from which the common beam grew a
    /// non-finite ellipse for ever.
    #[test]
    fn a_nearly_circular_scaled_ellipse_keeps_its_scale() {
        let (major, minor): (f64, f64) = (4.471_597_567_379_012e-7, 2.275_313_022_046_005e-7);
        // Scale it to a circle of the equal-area axis, then stretch x by a
        // part in a million.
        let equal = (major * minor).sqrt();
        let (x_scale, y_scale) = (equal / major * (1.0 + 1.0e-6), equal / minor);
        let (axis1, axis2, _) = transform_ellipse_by_scaling(major, minor, 0.0, x_scale, y_scale);
        assert!(
            (axis1 / equal - 1.0).abs() < 1.0e-5 && (axis2 / equal - 1.0).abs() < 1.0e-5,
            "{axis1:e}, {axis2:e} for a circle of {equal:e}"
        );
    }

    /// The common beam of a block of a cube's I and V beams, about 0.09 by
    /// 0.05 arcsec (radians), is found, and is the set's beam in arcseconds,
    /// the unit CASA's `getCommonBeam` works in, within one step of the
    /// 0.1% growth by which that algorithm encloses a pair (rounding in
    /// either unit can take one more). It once grew a non-finite ellipse for
    /// ever; it runs on a thread here so that a regression fails rather than
    /// hangs.
    #[test]
    fn the_common_beam_of_sub_arcsecond_beams_is_found_whatever_the_unit() {
        let beams = [
            (
                4.471_597_567_379_012e-7,
                2.275_313_022_046_005e-7,
                -0.006_651_467_446_095_271,
            ),
            (
                4.459_097_676_174_015e-7,
                2.269_180_481_074_049e-7,
                -0.006_636_139_185_516_454,
            ),
            (
                4.446_663_714_896_679e-7,
                2.263_097_828_255_912e-7,
                -0.006_620_643_437_072_206,
            ),
            (
                4.434_288_632_217_844e-7,
                2.257_043_733_320_881e-7,
                -0.006_610_111_467_198_782,
            ),
            (
                4.422_028_486_204_345e-7,
                2.251_034_414_326_026e-7,
                -0.006_595_494_249_825_071,
            ),
            (
                4.409_816_641_795_604e-7,
                2.245_055_063_480_108e-7,
                -0.006_582_559_193_186_771,
            ),
            (
                4.397_669_669_615_152e-7,
                2.239_118_549_458_848e-7,
                -0.006_568_831_950_153_904,
            ),
            (
                4.385_620_358_343_588e-7,
                2.233_217_644_649_855e-7,
                -0.006_560_902_283_978_464,
            ),
            (
                4.373_624_989_740_112e-7,
                2.227_353_759_318_961e-7,
                -0.006_543_279_647_702_705,
            ),
            (
                4.361_700_839_561_17e-7,
                2.221_524_072_934_501e-7,
                -0.006_530_641_075_795_137,
            ),
            (
                4.349_845_439_841_554e-7,
                2.215_731_582_311_37e-7,
                -0.006_516_701_091_964_254,
            ),
            (
                4.338_077_476_603_544e-7,
                2.209_976_992_582_482e-7,
                -0.006_506_659_622_268_164,
            ),
            (
                4.324_544_213_109_895e-7,
                2.204_539_888_949_102e-7,
                -0.006_672_069_493_840_556,
            ),
            (
                4.312_872_853_081_4e-7,
                2.198_860_748_442_245e-7,
                -0.006_660_263_679_922_289,
            ),
            (
                4.298_355_576_603_232e-7,
                2.193_680_489_473_581e-7,
                -0.006_413_735_586_021_764,
            ),
        ];
        let arcsec = std::f64::consts::PI / (180.0 * 3_600.0);
        let set = move |unit: f64| {
            beams
                .iter()
                .flat_map(|&(major, minor, angle)| {
                    // Each beam twice, as Stokes I and V share it.
                    [EllipticalGaussian::new(major / unit, minor / unit, angle); 2]
                })
                .collect::<Vec<_>>()
        };
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let found = (
                common_enclosing_gaussian(&set(1.0), 1.0),
                common_enclosing_gaussian(&set(arcsec), arcsec),
            );
            sender.send(found).expect("the test waits");
        });
        let (radians, arcseconds) = receiver
            .recv_timeout(std::time::Duration::from_secs(20))
            .expect("the common beam is found");
        let radians = radians.expect("a common beam in radians");
        let arcseconds = arcseconds.expect("a common beam in arcseconds");
        let step = 1.001_f64 * (1.0 + 1.0e-9) - 1.0;
        assert!(
            (radians.major / arcsec / arcseconds.major - 1.0).abs() <= step
                && (radians.minor / arcsec / arcseconds.minor - 1.0).abs() <= step
                && (radians.position_angle - arcseconds.position_angle).abs() < 1.0e-9,
            "{radians:?} in radians, {arcseconds:?} in arcseconds"
        );
        for beam in set(1.0) {
            assert!(
                beam == radians || encloses(radians, beam),
                "{radians:?} encloses {beam:?}"
            );
        }
    }

    #[test]
    fn covariance_deconvolution_round_trips_an_axis_aligned_pair() {
        let source = EllipticalGaussian::new(4.0e-6, 2.0e-6, 0.0);
        let target = EllipticalGaussian::new(5.0e-6, 3.0e-6, 0.0);
        let smoothing = deconvolving_gaussian(target, source)
            .expect("deconvolution")
            .expect("nonzero smoothing beam");
        assert!((smoothing.major.hypot(source.major) - target.major).abs() < 1.0e-15);
        assert!((smoothing.minor.hypot(source.minor) - target.minor).abs() < 1.0e-15);
    }
}
