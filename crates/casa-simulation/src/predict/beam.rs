// SPDX-License-Identifier: LGPL-3.0-or-later
//! The primary beam `sm.setvp(dovp=True)` applies: CASA's default voltage
//! patterns, as `PBMath` gives them by telescope name.

use casa_numerics::AnnularApertureVoltageTable;
use ndarray::Array2;

use super::SkyModel;

/// The primary-beam voltage pattern `setvp(dovp=True)` applies.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PrimaryBeam {
    /// CASA `PBMath1DAiry`: an Airy pattern for a dish with a central
    /// blockage, tabulated out to a maximum radius.
    Airy {
        /// Effective dish diameter in metres.
        dish_diameter_m: f64,
        /// Blockage diameter in metres.
        blockage_diameter_m: f64,
        /// Radius of the tabulated pattern at 1 GHz, in arcmin (scaled as
        /// 1/frequency). The pattern is zero beyond it, and CASA's 10,000
        /// table samples span it, so it also sets the lookup quantization.
        maximum_radius_arcmin_ghz: f64,
    },
}

impl PrimaryBeam {
    /// ALMA 12 m antennas as CASA's default voltage patterns give them
    /// (`PBMath::initByTelescope`): 10.7 m effective diameter, 0.75 m
    /// blockage, 1.784° at 1 GHz.
    pub const ALMA: Self = Self::Airy {
        dish_diameter_m: 10.7,
        blockage_diameter_m: 0.75,
        maximum_radius_arcmin_ghz: 1.784 * 60.0,
    };

    /// ACA 7 m antennas as CASA's default voltage patterns give them
    /// (`PBMath::initByTelescope`): 6.25 m effective diameter, 0.75 m
    /// blockage, 3.568° at 1 GHz.
    pub const ACA: Self = Self::Airy {
        dish_diameter_m: 6.25,
        blockage_diameter_m: 0.75,
        maximum_radius_arcmin_ghz: 3.568 * 60.0,
    };

    /// CASA's default voltage pattern for `telescope`, the OBSERVATION
    /// table's TELESCOPE_NAME, where it is one of these Airy patterns:
    /// `PBMath::enumerateCommonPB` matches the name's prefix, `ALMA` (which
    /// also catches `ALMASD`, whose pattern is ALMA's) and `ACA`, and maps
    /// `ATF` to ALMA. `None` for every other telescope, whose patterns
    /// (VLA's polynomial among them) casa-rs does not yet have.
    #[must_use]
    pub fn for_telescope(telescope: &str) -> Option<Self> {
        if telescope.starts_with("ALMA") || telescope.starts_with("ATF") {
            Some(Self::ALMA)
        } else if telescope.starts_with("ACA") {
            Some(Self::ACA)
        } else {
            None
        }
    }

    /// The pattern's lookup table, shared by every plane it is evaluated
    /// on.
    pub(crate) fn pattern(self) -> BeamPattern {
        let Self::Airy {
            dish_diameter_m,
            blockage_diameter_m,
            maximum_radius_arcmin_ghz,
        } = self;
        BeamPattern {
            table: AnnularApertureVoltageTable::new(
                dish_diameter_m,
                blockage_diameter_m,
                maximum_radius_arcmin_ghz,
            ),
        }
    }
}

/// A primary beam's tabulated voltage pattern.
#[derive(Clone)]
pub(crate) struct BeamPattern {
    table: AnnularApertureVoltageTable,
}

impl BeamPattern {
    /// Bytes the pattern holds.
    pub(crate) const BYTES: u64 = AnnularApertureVoltageTable::BYTES as u64;

    /// The power pattern on model plane `plane`, `[y][x]`, centred on the
    /// pixel `centre`, at the plane's frequency (`BeamSkyJones::apply`
    /// squares the voltage pattern).
    pub(super) fn power_plane(
        &self,
        model: &SkyModel,
        plane: usize,
        centre: [f64; 2],
    ) -> Array2<f32> {
        let [nx, ny] = model.shape();
        let ghz = model.plane_frequency_hz(plane) / 1.0e9;
        let increment = model.increment_rad;
        Array2::from_shape_fn((ny, nx), |(y, x)| {
            // PBMath1D works in `Float` degrees: keep its rounding of the
            // radius at the lookup boundaries.
            let l_deg = ((x as f64 - centre[0]) * increment[0]).to_degrees();
            let m_deg = ((y as f64 - centre[1]) * increment[1]).to_degrees();
            let [l_deg, m_deg] = [l_deg as f32, m_deg as f32];
            let radius_deg = (l_deg * l_deg + m_deg * m_deg).sqrt();
            let radius = (f64::from(radius_deg) * 60.0 * ghz) as f32;
            let voltage = self.table.evaluate(f64::from(radius));
            voltage * voltage
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telescopes_match_casas_common_beams_by_prefix() {
        assert_eq!(PrimaryBeam::for_telescope("ALMA"), Some(PrimaryBeam::ALMA));
        assert_eq!(
            PrimaryBeam::for_telescope("ALMASD"),
            Some(PrimaryBeam::ALMA)
        );
        assert_eq!(PrimaryBeam::for_telescope("ATF"), Some(PrimaryBeam::ALMA));
        assert_eq!(PrimaryBeam::for_telescope("ACA"), Some(PrimaryBeam::ACA));
        assert_eq!(PrimaryBeam::for_telescope("ACA-7m"), Some(PrimaryBeam::ACA));
        for other in ["VLA", "EVLA", "alma", "Unknown", "", "SMA", "AL"] {
            assert_eq!(PrimaryBeam::for_telescope(other), None, "{other}");
        }
    }
}
