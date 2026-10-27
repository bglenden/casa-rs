// SPDX-License-Identifier: LGPL-3.0-or-later
//! An antenna's pointing direction at a visibility time, chosen by an
//! [`ObservationPointingLaw`] from the pointing samples around that time.

use crate::geometry::{
    DirectionFrame, MissingPointingPolicy, ObservationPointingLaw, PointingExtrapolation,
    PointingInterpolation, SkyDirection,
};

/// One pointing sample evaluated at a query time.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PointingSample {
    /// The sample's identity: two with the same identity are one sample.
    pub sample: usize,
    /// The sample's own time, in seconds on the query's scale.
    pub time_s: f64,
    /// The direction at the query time, J2000 `[ra, dec]` in radians.
    pub direction_rad: [f64; 2],
}

/// The pointing samples around one (antenna, time) query.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct PointingSamples {
    /// The sample whose interval holds the time.
    pub covering: Option<PointingSample>,
    /// The latest sample at or before the time.
    pub before: Option<PointingSample>,
    /// The earliest sample at or after the time.
    pub after: Option<PointingSample>,
}

/// Why a pointing law gives no direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PointingLawError {
    /// The time is beyond the samples and the law does not hold the
    /// nearest.
    #[error("the time is outside the pointing samples' coverage")]
    OutsideCoverage,
    /// The antenna has no samples and the law does not fall back to the
    /// phase-tracking centre.
    #[error("the antenna has no pointing samples")]
    Missing,
    /// The interpolation fraction or result is not finite.
    #[error("the pointing interpolation is not finite")]
    InvalidInterpolation,
}

impl ObservationPointingLaw {
    /// The direction at `time_s` from `samples`: the covering sample, else
    /// the bracketing pair (nearest, or interpolated on the shorter great
    /// circle), else the nearest endpoint if the law holds it, else
    /// `fallback` (the phase-tracking centre) if the law allows it.
    ///
    /// # Errors
    ///
    /// [`PointingLawError`] where the law gives no direction.
    pub fn resolve(
        self,
        samples: PointingSamples,
        time_s: f64,
        fallback: SkyDirection,
    ) -> Result<SkyDirection, PointingLawError> {
        if let Some(covering) = samples.covering {
            return Ok(j2000(covering.direction_rad));
        }
        let direction = match (samples.before, samples.after) {
            (Some(before), Some(after)) if before.sample == after.sample => before.direction_rad,
            (Some(before), Some(after)) => match self.interpolation() {
                PointingInterpolation::Nearest => {
                    if time_s - before.time_s <= after.time_s - time_s {
                        before.direction_rad
                    } else {
                        after.direction_rad
                    }
                }
                PointingInterpolation::GreatCircleShortestArc => interpolate(
                    before.direction_rad,
                    after.direction_rad,
                    (time_s - before.time_s) / (after.time_s - before.time_s),
                )?,
            },
            (Some(endpoint), None) | (None, Some(endpoint))
                if self.extrapolation() == PointingExtrapolation::HoldNearest =>
            {
                endpoint.direction_rad
            }
            (Some(_), None) | (None, Some(_)) => return Err(PointingLawError::OutsideCoverage),
            (None, None) if self.missing() == MissingPointingPolicy::UsePhaseTrackingCentre => {
                return Ok(fallback);
            }
            (None, None) => return Err(PointingLawError::Missing),
        };
        Ok(j2000(direction))
    }
}

const fn j2000(direction_rad: [f64; 2]) -> SkyDirection {
    SkyDirection::new(DirectionFrame::J2000, direction_rad[0], direction_rad[1])
}

/// The direction `fraction` of the way from `before` to `after` along the
/// shorter great circle.
fn interpolate(
    before: [f64; 2],
    after: [f64; 2],
    fraction: f64,
) -> Result<[f64; 2], PointingLawError> {
    if !(fraction.is_finite() && (0.0..=1.0).contains(&fraction)) {
        return Err(PointingLawError::InvalidInterpolation);
    }
    let left = unit(before);
    let right = unit(after);
    let cosine = left
        .iter()
        .zip(right)
        .map(|(left, right)| left * right)
        .sum::<f64>()
        .clamp(-1.0, 1.0);
    let angle = cosine.acos();
    let vector = if angle.abs() < 1.0e-15 {
        left
    } else {
        let denominator = angle.sin();
        let before_weight = ((1.0 - fraction) * angle).sin() / denominator;
        let after_weight = (fraction * angle).sin() / denominator;
        [
            before_weight * left[0] + after_weight * right[0],
            before_weight * left[1] + after_weight * right[1],
            before_weight * left[2] + after_weight * right[2],
        ]
    };
    let norm = vector.iter().map(|value| value * value).sum::<f64>().sqrt();
    if !(norm.is_finite() && norm > 0.0) {
        return Err(PointingLawError::InvalidInterpolation);
    }
    let [x, y, z] = vector.map(|value| value / norm);
    Ok([y.atan2(x).rem_euclid(std::f64::consts::TAU), z.asin()])
}

fn unit(direction: [f64; 2]) -> [f64; 3] {
    [
        direction[1].cos() * direction[0].cos(),
        direction[1].cos() * direction[0].sin(),
        direction[1].sin(),
    ]
}

#[cfg(test)]
mod tests {
    use super::{PointingLawError, PointingSample, PointingSamples};
    use crate::geometry::{
        DirectionFrame, MissingPointingPolicy, ObservationPointingLaw, PointingDirectionColumn,
        PointingDirectionSemantic, PointingExtrapolation, PointingInterpolation,
        PointingTimeSampling, SkyDirection,
    };

    fn law(
        interpolation: PointingInterpolation,
        extrapolation: PointingExtrapolation,
        missing: MissingPointingPolicy,
    ) -> ObservationPointingLaw {
        ObservationPointingLaw::new(
            PointingDirectionColumn::Direction,
            PointingDirectionSemantic::AntennaBoresight,
            PointingTimeSampling::VisibilityTime,
            interpolation,
            extrapolation,
            missing,
        )
    }

    fn sample(sample: usize, time_s: f64, ra: f64) -> Option<PointingSample> {
        Some(PointingSample {
            sample,
            time_s,
            direction_rad: [ra, 0.0],
        })
    }

    const FALLBACK: SkyDirection = SkyDirection::new(DirectionFrame::J2000, 3.0, 0.5);

    /// A covering sample wins; otherwise the bracketing pair gives the
    /// nearest (the earlier on a tie) or the great-circle interpolation.
    #[test]
    fn covering_then_bracketing_samples_give_the_direction() {
        let nearest = law(
            PointingInterpolation::Nearest,
            PointingExtrapolation::Reject,
            MissingPointingPolicy::Reject,
        );
        let around = PointingSamples {
            covering: None,
            before: sample(0, 10.0, 0.1),
            after: sample(1, 20.0, 0.3),
        };
        let covered = PointingSamples {
            covering: sample(2, 14.0, 0.7),
            ..around
        };
        assert_eq!(
            nearest
                .resolve(covered, 15.0, FALLBACK)
                .unwrap()
                .longitude_rad(),
            0.7
        );
        assert_eq!(
            nearest
                .resolve(around, 15.0, FALLBACK)
                .unwrap()
                .longitude_rad(),
            0.1
        );
        assert_eq!(
            nearest
                .resolve(around, 16.0, FALLBACK)
                .unwrap()
                .longitude_rad(),
            0.3
        );
        let arc = law(
            PointingInterpolation::GreatCircleShortestArc,
            PointingExtrapolation::Reject,
            MissingPointingPolicy::Reject,
        );
        let middle = arc.resolve(around, 15.0, FALLBACK).unwrap();
        assert!((middle.longitude_rad() - 0.2).abs() < 1e-15);
        assert!(middle.latitude_rad().abs() < 1e-15);
    }

    /// Beyond the samples the law holds the nearest or refuses; without
    /// samples it falls back to the phase-tracking centre or refuses.
    #[test]
    fn endpoints_and_missing_samples_follow_the_law() {
        let after_only = PointingSamples {
            after: sample(0, 20.0, 0.3),
            ..PointingSamples::default()
        };
        let hold = law(
            PointingInterpolation::Nearest,
            PointingExtrapolation::HoldNearest,
            MissingPointingPolicy::UsePhaseTrackingCentre,
        );
        assert_eq!(
            hold.resolve(after_only, 5.0, FALLBACK)
                .unwrap()
                .longitude_rad(),
            0.3
        );
        assert_eq!(
            hold.resolve(PointingSamples::default(), 5.0, FALLBACK)
                .unwrap(),
            FALLBACK
        );
        let reject = law(
            PointingInterpolation::Nearest,
            PointingExtrapolation::Reject,
            MissingPointingPolicy::Reject,
        );
        assert_eq!(
            reject.resolve(after_only, 5.0, FALLBACK),
            Err(PointingLawError::OutsideCoverage)
        );
        assert_eq!(
            reject.resolve(PointingSamples::default(), 5.0, FALLBACK),
            Err(PointingLawError::Missing)
        );
    }
}
