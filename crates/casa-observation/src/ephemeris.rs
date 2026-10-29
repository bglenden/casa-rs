// SPDX-License-Identifier: LGPL-3.0-or-later
//! The ephemeris a field follows: a body's geocentric direction, distance
//! and radial velocity, sampled in time.
//!
//! It is what MSv4's `field_and_source_ephemeris` dataset holds, and what
//! casacore attaches to an MSv2 field (`FIELD/EPHEM<id>_<name>.tab`, named
//! by the field's `EPHEMERIS_ID`). Casacore reads such a field's phase
//! centre ([`crate::FieldEntry::phase_centre`]) as an offset from the
//! body's direction, shifted by its true angle; CASA's imager evaluates
//! that direction from the observatory, its samples interpolated at the
//! time's TDB ([`casa_types::measures::ephemeris`]).

use std::f64::consts::{PI, TAU};
use std::mem::size_of;

use casa_types::measures::direction::DirectionRef;
use casa_types::measures::radial_velocity::RadialVelocityRef;

use crate::ObservationError;

/// A body's geocentric samples, in time order.
///
/// Built only through [`Self::new`], which checks them, so every
/// ephemeris has at least two samples, at strictly increasing finite
/// times, with finite directions and velocities and positive distances.
/// Its last samples may be extrapolated rather than the store's
/// ([`Self::extrapolated_to`], [`Self::extrapolated`]).
#[derive(Clone, Debug, PartialEq)]
pub struct FieldEphemeris {
    name: String,
    times_mjd: Vec<f64>,
    directions_rad: Vec<[f64; 2]>,
    frame: DirectionRef,
    distances_m: Vec<f64>,
    radial_velocities_m_s: Vec<f64>,
    velocity_frame: RadialVelocityRef,
    extrapolated: usize,
}

/// An ephemeris at one time.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EphemerisSample {
    /// The body's geocentric position, metres, along the axes of the
    /// ephemeris's frame ([`FieldEphemeris::frame`]).
    pub position_m: [f64; 3],
    /// The body's radial velocity, metres a second, in the ephemeris's
    /// velocity frame ([`FieldEphemeris::velocity_frame`]).
    pub radial_velocity_m_s: f64,
}

impl FieldEphemeris {
    /// The ephemeris of body `name`: at each of `times_mjd` (days, as the
    /// store states them), its geocentric direction (`directions_rad`,
    /// longitude and latitude in `frame`), distance (`distances_m`) and
    /// radial velocity (`radial_velocities_m_s`, in `velocity_frame`).
    ///
    /// # Errors
    ///
    /// [`ObservationError::InvalidRequest`] when the four series differ in
    /// length, hold fewer than two samples, a time is not finite or not
    /// after the one before, a direction or velocity is not finite, a
    /// distance is not positive, or `frame` is neither ICRS nor J2000.
    pub fn new(
        name: impl Into<String>,
        times_mjd: Vec<f64>,
        directions_rad: Vec<[f64; 2]>,
        frame: DirectionRef,
        distances_m: Vec<f64>,
        (radial_velocities_m_s, velocity_frame): (Vec<f64>, RadialVelocityRef),
    ) -> Result<Self, ObservationError> {
        let name = name.into();
        let invalid = |reason: &str| ObservationError::InvalidRequest {
            reason: format!("the ephemeris of {name}: {reason}"),
        };
        let samples = times_mjd.len();
        if directions_rad.len() != samples
            || distances_m.len() != samples
            || radial_velocities_m_s.len() != samples
        {
            return Err(invalid("its series differ in length"));
        }
        if samples < 2 {
            return Err(invalid("it has fewer than two samples"));
        }
        if !matches!(frame, DirectionRef::ICRS | DirectionRef::J2000) {
            return Err(invalid("its directions are neither ICRS nor J2000"));
        }
        for sample in 0..samples {
            let time = times_mjd[sample];
            if !time.is_finite() || (sample > 0 && times_mjd[sample - 1] >= time) {
                return Err(invalid(&format!(
                    "sample {sample}'s time is not finite and after the one before"
                )));
            }
            if directions_rad[sample]
                .iter()
                .any(|angle| !angle.is_finite())
                || !radial_velocities_m_s[sample].is_finite()
                || !(distances_m[sample].is_finite() && distances_m[sample] > 0.0)
            {
                return Err(invalid(&format!(
                    "sample {sample} has no finite direction, velocity and positive distance"
                )));
            }
        }
        Ok(Self {
            name,
            times_mjd,
            directions_rad,
            frame,
            distances_m,
            radial_velocities_m_s,
            velocity_frame,
            extrapolated: 0,
        })
    }

    /// This ephemeris with a sample more at each of `times_mjd`, after its
    /// last: each series continued along the straight line through its
    /// last two samples, directions in angle, as XRADIO interpolates
    /// between samples (a longitude crossing a turn continues from the last
    /// one's). The samples are not the store's, so [`Self::extrapolated`]
    /// counts them.
    ///
    /// # Errors
    ///
    /// [`ObservationError::InvalidRequest`] when `times_mjd` are not
    /// increasing after the last sample, or the line reaches no positive
    /// distance at one.
    pub fn extrapolated_to(&self, times_mjd: &[f64]) -> Result<Self, ObservationError> {
        let last = self.times_mjd.len() - 1;
        let span = self.times_mjd[last] - self.times_mjd[last - 1];
        let [[longitude_0, latitude_0], [longitude_1, latitude_1]] =
            [self.directions_rad[last - 1], self.directions_rad[last]];
        let turn = (longitude_1 - longitude_0 + PI).rem_euclid(TAU) - PI;
        let (mut times, mut directions, mut distances, mut velocities) = (
            self.times_mjd.clone(),
            self.directions_rad.clone(),
            self.distances_m.clone(),
            self.radial_velocities_m_s.clone(),
        );
        for time in times_mjd {
            let factor = (time - self.times_mjd[last - 1]) / span;
            let along =
                |series: &[f64]| series[last - 1] + factor * (series[last] - series[last - 1]);
            times.push(*time);
            directions.push([
                longitude_1 + (factor - 1.0) * turn,
                latitude_0 + factor * (latitude_1 - latitude_0),
            ]);
            distances.push(along(&self.distances_m));
            velocities.push(along(&self.radial_velocities_m_s));
        }
        let mut extended = Self::new(
            self.name.clone(),
            times,
            directions,
            self.frame,
            distances,
            (velocities, self.velocity_frame),
        )?;
        extended.extrapolated = self.extrapolated + times_mjd.len();
        Ok(extended)
    }

    /// How many of the last samples are extrapolated
    /// ([`Self::extrapolated_to`]) rather than the store's: none unless a
    /// reader added them.
    #[must_use]
    pub const fn extrapolated(&self) -> usize {
        self.extrapolated
    }

    /// The body's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Each sample's time, MJD days as the store states it (MSv2 `MJD`).
    /// Casacore's `MeasComet` reads it on whatever scale its caller
    /// evaluates in: CASA's imager evaluates directions at a time's TDB
    /// and radial velocities at its UTC.
    #[must_use]
    pub fn times_mjd(&self) -> &[f64] {
        &self.times_mjd
    }

    /// Each sample's geocentric (longitude, latitude), radians, in
    /// [`Self::frame`].
    #[must_use]
    pub fn directions_rad(&self) -> &[[f64; 2]] {
        &self.directions_rad
    }

    /// The directions' frame: ICRS or J2000.
    #[must_use]
    pub const fn frame(&self) -> DirectionRef {
        self.frame
    }

    /// Each sample's geocentric distance, metres.
    #[must_use]
    pub fn distances_m(&self) -> &[f64] {
        &self.distances_m
    }

    /// Each sample's radial velocity, metres a second, in
    /// [`Self::velocity_frame`].
    #[must_use]
    pub fn radial_velocities_m_s(&self) -> &[f64] {
        &self.radial_velocities_m_s
    }

    /// The radial velocities' frame: GEO for an ephemeris whose observer is
    /// the Earth's centre, TOPO for one stated from a point away from it
    /// (casacore: `GeoDist` above a metre).
    #[must_use]
    pub const fn velocity_frame(&self) -> RadialVelocityRef {
        self.velocity_frame
    }

    /// The ephemeris at `time_mjd`, or `None` outside its samples: a
    /// sample's own values at its time, else the linear interpolation, in
    /// time, of the Cartesian positions and of the velocities of the
    /// samples either side, as casacore's `MeasComet` interpolates.
    #[must_use]
    pub fn sample(&self, time_mjd: f64) -> Option<EphemerisSample> {
        let last = self.times_mjd.len() - 1;
        if !time_mjd.is_finite() || time_mjd < self.times_mjd[0] || time_mjd > self.times_mjd[last]
        {
            return None;
        }
        let upper = self.times_mjd.partition_point(|time| *time < time_mjd);
        if upper <= last && self.times_mjd[upper] == time_mjd {
            return Some(EphemerisSample {
                position_m: self.position_m(upper),
                radial_velocity_m_s: self.radial_velocities_m_s[upper],
            });
        }
        let lower = upper.saturating_sub(1);
        let factor =
            (time_mjd - self.times_mjd[lower]) / (self.times_mjd[upper] - self.times_mjd[lower]);
        let [before, after] = [lower, upper].map(|sample| self.position_m(sample));
        Some(EphemerisSample {
            position_m: std::array::from_fn(|axis| interpolate(before[axis], after[axis], factor)),
            radial_velocity_m_s: interpolate(
                self.radial_velocities_m_s[lower],
                self.radial_velocities_m_s[upper],
                factor,
            ),
        })
    }

    /// Bytes the ephemeris holds beside itself: its name and samples.
    #[must_use]
    pub fn held_bytes(&self) -> usize {
        self.name.capacity()
            + self.times_mjd.capacity() * size_of::<f64>()
            + self.directions_rad.capacity() * size_of::<[f64; 2]>()
            + self.distances_m.capacity() * size_of::<f64>()
            + self.radial_velocities_m_s.capacity() * size_of::<f64>()
    }

    /// Sample `sample`'s geocentric position, metres.
    fn position_m(&self, sample: usize) -> [f64; 3] {
        let [longitude, latitude] = self.directions_rad[sample];
        let distance = self.distances_m[sample];
        let (sin_longitude, cos_longitude) = longitude.sin_cos();
        let (sin_latitude, cos_latitude) = latitude.sin_cos();
        [
            distance * cos_latitude * cos_longitude,
            distance * cos_latitude * sin_longitude,
            distance * sin_latitude,
        ]
    }
}

fn interpolate(before: f64, after: f64, factor: f64) -> f64 {
    before + factor * (after - before)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ephemeris() -> FieldEphemeris {
        FieldEphemeris::new(
            "Moon",
            vec![60_000.0, 60_000.5, 60_001.0],
            vec![[0.1, 0.2], [0.2, 0.25], [0.3, 0.2]],
            DirectionRef::ICRS,
            vec![3.8e8, 3.9e8, 4.0e8],
            (vec![10.0, 20.0, 40.0], RadialVelocityRef::GEO),
        )
        .expect("an ephemeris")
    }

    #[test]
    fn a_sample_time_gives_its_own_values_and_a_time_between_interpolates() {
        let ephemeris = ephemeris();
        let own = ephemeris.sample(60_000.5).expect("inside");
        assert_eq!(own.radial_velocity_m_s, 20.0);
        assert_eq!(own.position_m, ephemeris.position_m(1));
        let between = ephemeris.sample(60_000.75).expect("inside");
        assert_eq!(between.radial_velocity_m_s, 30.0);
        let [before, after] = [ephemeris.position_m(1), ephemeris.position_m(2)];
        for axis in 0..3 {
            assert_eq!(
                between.position_m[axis],
                before[axis] + 0.5 * (after[axis] - before[axis])
            );
        }
        assert!(ephemeris.sample(59_999.9).is_none());
        assert!(ephemeris.sample(60_001.1).is_none());
        assert!(ephemeris.sample(f64::NAN).is_none());
    }

    #[test]
    fn an_extrapolated_sample_continues_the_last_two_along_a_line() {
        let ephemeris = ephemeris();
        assert_eq!(ephemeris.extrapolated(), 0);
        let extended = ephemeris
            .extrapolated_to(&[60_001.25, 60_001.5])
            .expect("after the last");
        assert_eq!(extended.extrapolated(), 2);
        assert_eq!(
            extended.times_mjd(),
            [60_000.0, 60_000.5, 60_001.0, 60_001.25, 60_001.5]
        );
        let close = |a: f64, b: f64| assert!((a - b).abs() < 1.0e-12, "{a} and {b}");
        close(extended.directions_rad()[3][0], 0.35);
        close(extended.directions_rad()[3][1], 0.175);
        close(extended.distances_m()[3], 4.05e8);
        close(extended.radial_velocities_m_s()[3], 50.0);
        close(extended.directions_rad()[4][0], 0.4);
        close(extended.radial_velocities_m_s()[4], 60.0);
        // Longitude continues from the last across a turn, not back around.
        let across = FieldEphemeris::new(
            "Moon",
            vec![60_000.0, 60_000.5],
            vec![[TAU - 0.1, 0.0], [0.0, 0.0]],
            DirectionRef::ICRS,
            vec![3.8e8; 2],
            (vec![0.0; 2], RadialVelocityRef::GEO),
        )
        .expect("an ephemeris")
        .extrapolated_to(&[60_001.0])
        .expect("after the last");
        close(across.directions_rad()[2][0], 0.1);
        assert!(ephemeris.extrapolated_to(&[60_000.75]).is_err());
        assert!(ephemeris.extrapolated_to(&[60_001.5, 60_001.25]).is_err());
    }

    #[test]
    fn construction_refuses_samples_it_could_not_interpolate() {
        let refused = |times: Vec<f64>, distances: Vec<f64>, frame: DirectionRef| {
            let samples = times.len();
            FieldEphemeris::new(
                "body",
                times,
                vec![[0.0, 0.0]; samples],
                frame,
                distances,
                (vec![0.0; samples], RadialVelocityRef::GEO),
            )
            .is_err()
        };
        assert!(refused(vec![1.0], vec![1.0], DirectionRef::ICRS));
        assert!(refused(vec![2.0, 1.0], vec![1.0, 1.0], DirectionRef::ICRS));
        assert!(refused(vec![1.0, 1.0], vec![1.0, 1.0], DirectionRef::ICRS));
        assert!(refused(vec![1.0, 2.0], vec![1.0, 0.0], DirectionRef::ICRS));
        assert!(refused(vec![1.0, 2.0], vec![1.0, 1.0], DirectionRef::APP));
        assert!(!refused(
            vec![1.0, 2.0],
            vec![1.0, 1.0],
            DirectionRef::J2000
        ));
    }
}
