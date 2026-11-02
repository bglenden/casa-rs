// SPDX-License-Identifier: LGPL-3.0-or-later
//! The angle an antenna's mount turns its feed by on the sky
//! ([`MsCalEngine::feed_parallactic_angle`]).

use super::MsCalEngine;
use crate::error::MsResult;

/// How an antenna's mount turns its feed, from `ANTENNA::MOUNT` as casacore's
/// `MSDerivedValues::setAntennaMount` reads it: the names in all lower or
/// all upper case, an empty mount as alt-az.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FeedMount {
    /// `ALT-AZ`, `ALT-AZ+ROTATOR` or no mount.
    AltAz,
    /// `EQUATORIAL`: the feed keeps its angle to the sky.
    Equatorial,
    /// `X-Y`.
    XY,
    /// `ALT-AZ+NASMYTH-R`.
    NasmythRight,
    /// `ALT-AZ+NASMYTH-L`.
    NasmythLeft,
    /// `ALT-AZ+BWG-R`, a right-handed beam waveguide.
    BeamWaveguideRight,
    /// `ALT-AZ+BWG-L`.
    BeamWaveguideLeft,
    /// `ORBITING` or a mount casacore does not know; it warns and uses no
    /// rotation.
    Unhandled,
}

impl FeedMount {
    pub(crate) fn parse(mount: &str) -> Self {
        match mount {
            "" | "alt-az" | "ALT-AZ" | "alt-az+rotator" | "ALT-AZ+ROTATOR" => Self::AltAz,
            "equatorial" | "EQUATORIAL" => Self::Equatorial,
            "x-y" | "X-Y" => Self::XY,
            "alt-az+nasmyth-r" | "ALT-AZ+NASMYTH-R" => Self::NasmythRight,
            "alt-az+nasmyth-l" | "ALT-AZ+NASMYTH-L" => Self::NasmythLeft,
            "alt-az+bwg-r" | "ALT-AZ+BWG-R" => Self::BeamWaveguideRight,
            "alt-az+bwg-l" | "ALT-AZ+BWG-L" => Self::BeamWaveguideLeft,
            _ => Self::Unhandled,
        }
    }
}

impl MsCalEngine {
    /// The angle (radians) the antenna's mount turns its feed by on the sky
    /// at `time_mjd_sec`, towards `field_id`, before its receptor angle: what
    /// CASA's visibility iterator calls the feed parallactic angle
    /// (`VisibilityIteratorImpl2::feed_pa`, from casacore
    /// `MSDerivedValues::parAngle`).
    ///
    /// For an alt-az mount it is the parallactic angle χ. A Nasmyth mount
    /// adds the elevation E (right-handed) or subtracts it (left-handed), and
    /// a beam-waveguide mount adds E − A, A the azimuth (right-handed), or
    /// subtracts it (left-handed). An X-Y mount turns by
    /// atan2(−cos H, −sin H sin δ), H and δ the hour angle and declination.
    /// An equatorial mount, an orbiting one and an unknown one give 0.
    ///
    /// [`Self::parallactic_angle`] is χ for every `ALT-AZ…` mount and 0
    /// otherwise, as casacore's derived MS columns compute it
    /// (`MSCalEngine::getPA`); the two differ for Nasmyth, beam-waveguide and
    /// X-Y mounts and for an empty `MOUNT`.
    ///
    /// # Errors
    ///
    /// An antenna or field index out of range, or a direction conversion
    /// that fails.
    pub fn feed_parallactic_angle(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
        antenna_id: usize,
    ) -> MsResult<f64> {
        let mount = self
            .antenna_feed_mounts
            .get(antenna_id)
            .copied()
            .ok_or_else(|| crate::error::MsError::InvalidIndex {
                index: antenna_id,
                max: self.antenna_feed_mounts.len(),
                context: "antenna_id".to_string(),
            })?;
        let sky = |engine: &Self| engine.sky_parallactic_angle(time_mjd_sec, field_id, antenna_id);
        let azel = |engine: &Self| engine.azel(time_mjd_sec, field_id, antenna_id);
        Ok(match mount {
            FeedMount::AltAz => sky(self)?,
            FeedMount::NasmythRight => sky(self)? + azel(self)?.1,
            FeedMount::NasmythLeft => sky(self)? - azel(self)?.1,
            FeedMount::BeamWaveguideRight => {
                let (azimuth, elevation) = azel(self)?;
                sky(self)? + elevation - azimuth
            }
            FeedMount::BeamWaveguideLeft => {
                let (azimuth, elevation) = azel(self)?;
                sky(self)? - (elevation - azimuth)
            }
            FeedMount::XY => {
                let (hour_angle, declination) = self.hadec(time_mjd_sec, field_id, antenna_id)?;
                (-hour_angle.cos()).atan2(-hour_angle.sin() * declination.sin())
            }
            FeedMount::Equatorial | FeedMount::Unhandled => 0.0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_types::measures::direction::{DirectionRef, MDirection};
    use casa_types::measures::position::MPosition;

    #[test]
    fn mounts_parse_as_casacore_reads_them() {
        for (mount, expected) in [
            ("", FeedMount::AltAz),
            ("ALT-AZ", FeedMount::AltAz),
            ("alt-az", FeedMount::AltAz),
            ("ALT-AZ+ROTATOR", FeedMount::AltAz),
            ("EQUATORIAL", FeedMount::Equatorial),
            ("X-Y", FeedMount::XY),
            ("ALT-AZ+NASMYTH-R", FeedMount::NasmythRight),
            ("alt-az+nasmyth-l", FeedMount::NasmythLeft),
            ("ALT-AZ+BWG-R", FeedMount::BeamWaveguideRight),
            ("alt-az+bwg-l", FeedMount::BeamWaveguideLeft),
            ("ORBITING", FeedMount::Unhandled),
            ("SPACE-HALCA", FeedMount::Unhandled),
            // casacore compares whole names in one case only.
            ("Alt-Az", FeedMount::Unhandled),
        ] {
            assert_eq!(FeedMount::parse(mount), expected, "{mount:?}");
        }
    }

    #[test]
    fn each_mount_turns_the_feed_by_its_casacore_term() {
        // Seven antennas at the VLA, one per mount kind, towards Dec +45°.
        let position = MPosition::new_itrf(-1_601_185.4, -5_041_977.5, 3_554_875.9);
        let mut engine = MsCalEngine::from_parts(
            vec![position.clone(); 7],
            vec![MDirection::from_angles(
                0.3,
                std::f64::consts::FRAC_PI_4,
                DirectionRef::J2000,
            )],
            position,
            casa_test_support::deterministic_measures_provider(),
        );
        engine.antenna_feed_mounts = [
            "ALT-AZ",
            "ALT-AZ+NASMYTH-R",
            "ALT-AZ+NASMYTH-L",
            "ALT-AZ+BWG-R",
            "ALT-AZ+BWG-L",
            "EQUATORIAL",
            "X-Y",
        ]
        .map(FeedMount::parse)
        .into();
        let time = 59_000.5 * 86_400.0;
        let chi = engine.parallactic_angle(time, 0, 0).unwrap();
        let (azimuth, elevation) = engine.azel(time, 0, 0).unwrap();
        let (hour_angle, declination) = engine.hadec(time, 0, 0).unwrap();
        let feed = |antenna| engine.feed_parallactic_angle(time, 0, antenna).unwrap();
        assert_eq!(feed(0), chi);
        assert_eq!(feed(1), chi + elevation);
        assert_eq!(feed(2), chi - elevation);
        assert_eq!(feed(3), chi + elevation - azimuth);
        assert_eq!(feed(4), chi - (elevation - azimuth));
        assert_eq!(feed(5), 0.0);
        assert_eq!(
            feed(6),
            (-hour_angle.cos()).atan2(-hour_angle.sin() * declination.sin())
        );
        // The derived columns' angle ignores the mount's own turn.
        assert_eq!(engine.parallactic_angle(time, 0, 3).unwrap(), chi);
    }
}
