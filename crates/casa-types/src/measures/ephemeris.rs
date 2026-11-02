// SPDX-License-Identifier: LGPL-3.0-or-later
//! The direction of a body that moves across the sky, as CASA's imager
//! evaluates a field that follows an ephemeris: the body's geocentric
//! position seen from the observatory (casacore `MeasComet`'s topocentric
//! correction), and a phase centre offset from that direction.
//!
//! The arithmetic follows the evaluation casa-rs's MeasurementSet geometry
//! has always made, statement by statement, so that every route that
//! evaluates an ephemeris gets the same bits.

use super::direction::{DirectionRef, MDirection};
use super::epoch::EpochRef;
use super::error::MeasureError;
use super::frame::MeasFrame;
use super::position::MPosition;

/// The J2000 direction, from `observatory` at `frame`'s epoch, of a body
/// at `geocentric_m` (metres, in `reference`'s axes from the Earth's
/// centre).
///
/// The observatory is placed on the apparent equator at the local apparent
/// sidereal time and its geocentric latitude; the body's position is
/// shifted by it, converted to the apparent frame, and the observatory's
/// shift, scaled by the body's distance, is taken out again before the
/// direction is converted to J2000. `frame` holds the epoch, `observatory`
/// as its position, and the Measures provider.
///
/// # Errors
///
/// [`MeasureError::ModelError`] for a position whose distance is not
/// positive and finite; [`MeasureError::MissingFrameData`] for a frame
/// without an epoch; a failed conversion.
pub fn topocentric_j2000(
    geocentric_m: [f64; 3],
    reference: DirectionRef,
    observatory: &MPosition,
    frame: &MeasFrame,
) -> Result<MDirection, MeasureError> {
    let [x, y, z] = geocentric_m;
    let distance = x.hypot(y).hypot(z);
    if !distance.is_finite() || distance <= 0.0 {
        return Err(MeasureError::ModelError {
            model: "ephemeris",
            reason: "the position has no positive finite distance".to_string(),
        });
    }
    let [observer_x, observer_y, observer_z] = observatory.as_itrf();
    let observer_radius = observer_x.hypot(observer_y).hypot(observer_z);
    let observer_latitude = (observer_z / observer_radius).clamp(-1.0, 1.0).asin();
    let epoch = frame
        .epoch()
        .ok_or(MeasureError::MissingFrameData { what: "epoch" })?;
    let last_days = epoch.convert_to(EpochRef::LAST, frame)?.value().as_mjd();
    let last = last_days.fract() * 2.0 * std::f64::consts::PI;
    let (sin_last, cos_last) = last.sin_cos();
    let (sin_latitude, cos_latitude) = observer_latitude.sin_cos();
    let observer_app = [
        observer_radius * cos_latitude * cos_last,
        observer_radius * cos_latitude * sin_last,
        observer_radius * sin_latitude,
    ];
    let pre_topocentric = MDirection::from_cosines(
        [
            x + observer_app[0],
            y + observer_app[1],
            z + observer_app[2],
        ],
        reference,
    );
    let apparent = pre_topocentric.convert_to(DirectionRef::APP, frame)?;
    let apparent_cosines = apparent.cosines();
    MDirection::from_cosines(
        [
            apparent_cosines[0] - observer_app[0] / distance,
            apparent_cosines[1] - observer_app[1] / distance,
            apparent_cosines[2] - observer_app[2] / distance,
        ],
        DirectionRef::APP,
    )
    .convert_to(DirectionRef::J2000, frame)
}

/// `direction` offset by `offset_rad` (longitude, latitude) in its own
/// frame, as casacore reads a moving field's `PHASE_DIR`: the latitude
/// offset is added along the meridian, then the longitude offset turns the
/// result about the pole by its true angle.
///
/// # Errors
///
/// [`MeasureError::ModelError`] when the offset direction is not finite.
pub fn offset_true_angle(
    direction: &MDirection,
    offset_rad: [f64; 2],
) -> Result<MDirection, MeasureError> {
    let (longitude, latitude) = direction.as_angles();
    let [longitude_offset, latitude_offset] = offset_rad;
    let (sin_offset, cos_offset) = longitude_offset.sin_cos();
    let shifted_latitude = latitude + latitude_offset;
    let (sin_latitude, cos_latitude) = shifted_latitude.sin_cos();
    let (sin_longitude, cos_longitude) = longitude.sin_cos();
    let x = cos_offset * cos_latitude * cos_longitude - sin_offset * sin_longitude;
    let y = cos_offset * cos_latitude * sin_longitude + sin_offset * cos_longitude;
    let z = cos_offset * sin_latitude;
    if [x, y, z].iter().any(|value| !value.is_finite()) {
        return Err(MeasureError::ModelError {
            model: "ephemeris",
            reason: "a moving field's phase-centre offset is not finite".to_string(),
        });
    }
    Ok(MDirection::from_angles(
        y.atan2(x).rem_euclid(std::f64::consts::TAU),
        z.atan2(x.hypot(y)),
        direction.refer(),
    ))
}
