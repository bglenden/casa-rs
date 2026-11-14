// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's default primary beams, as `PBMath` gives them by telescope name
//! and frequency, and their radial power patterns.
//!
//! CASA's VLA patterns can move the RR and LL beams apart. Its EVLA
//! implementation currently leaves them unsquinted. Squint reaches an image
//! with RR and LL
//! planes: a Stokes I image is one plane, and CASA applies the unsquinted
//! pattern to it (measured on CASA 6.7.6: RR and LL agree, whatever the
//! offset's direction). These patterns are unsquinted.

use crate::{AnnularApertureVoltageTable, EvlaPowerPattern, VlaBand};

mod defaults;
mod gaussian;
mod sma;
mod squint;
mod tabulated;
pub use defaults::DefaultRadial;
use gaussian::GaussianVoltageTable;
use sma::SmaVoltageTable;

/// A primary-beam voltage pattern of CASA's.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PrimaryBeam {
    /// CASA's other sampled, cosine and polynomial telescope defaults.
    DefaultRadial(DefaultRadial),
    /// SMA's spheroidal power samples, sinc interpolated by PBMath1DNumeric.
    Sma,
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
    /// CASA `PBMath1DEVLA`: the EVLA's polynomial power patterns, each
    /// frequency taking the fit CASA picks for it ([`EvlaPowerPattern::at`]),
    /// tabulated to 58′ at 1 GHz.
    Evla,
    /// CASA `PBMath1DGauss`, with the stored HWHM and support normalized
    /// to arcmin at 1 GHz. A power pattern is square-rooted before storage.
    Gaussian {
        /// Stored HWHM times the reference frequency in GHz, arcmin GHz.
        half_width_arcmin_ghz: f64,
        /// Radius beyond which the pattern is zero, arcmin GHz.
        maximum_radius_arcmin_ghz: f64,
        /// Whether the stored Gaussian specifies voltage rather than power.
        is_voltage: bool,
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

    /// VLA antennas in a recognised band ([`VlaBand`]; `PBMath`'s `VLA_4`,
    /// `VLA_P`, `VLA_L`, `VLA_C`, `VLA_X`, `VLA_U`, `VLA_K` and `VLA_Q`):
    /// 25 m with a 2.36 m blockage, 0.8564° at 1 GHz.
    pub const VLA: Self = Self::Airy {
        dish_diameter_m: 25.0,
        blockage_diameter_m: 2.36,
        maximum_radius_arcmin_ghz: 0.8564 * 60.0,
    };

    /// VLA antennas outside the recognised bands (`VLA_NVSS`): 24.5 m
    /// without blockage, 0.8564° at 1 GHz.
    pub const VLA_NVSS: Self = Self::Airy {
        dish_diameter_m: 24.5,
        blockage_diameter_m: 0.0,
        maximum_radius_arcmin_ghz: 0.8564 * 60.0,
    };

    /// CASA's 18 m, unblocked ngVLA aperture, supported to 1.5° at 1 GHz.
    pub const NGVLA: Self = Self::Airy {
        dish_diameter_m: 18.,
        blockage_diameter_m: 0.,
        maximum_radius_arcmin_ghz: 90.,
    };

    /// CASA's ATA aperture: 6 m diameter, 0.5 m blockage, 3.568° at 1 GHz.
    pub const ATA: Self = Self::Airy {
        dish_diameter_m: 6.,
        blockage_diameter_m: 0.5,
        maximum_radius_arcmin_ghz: 3.568 * 60.,
    };

    /// CASA's default voltage pattern for `telescope`, the OBSERVATION
    /// table's TELESCOPE_NAME, at `reference_frequency_hz`, spectral window
    /// 0's REF_FREQUENCY (`VPSkyJones`), where it is one of these patterns.
    /// `PBMath::enumerateCommonPB` matches the name's prefix:
    /// - `ALMA` (which also catches `ALMASD`, whose pattern is ALMA's), and
    ///   `ATF`, which maps to ALMA;
    /// - `ACA`;
    /// - `VLA`, whose pattern follows the band of the reference frequency
    ///   ([`VlaBand::of`], `PBMath::whichCommonPBtoUse`): [`Self::VLA`] in
    ///   the bands CASA recognises and [`Self::VLA_NVSS`] between them;
    /// - `EVLA`: [`Self::Evla`], which picks its fit per frequency;
    /// - `NGVLA`, `ATA`, and `SMA`: their Airy or sampled spheroidal beams.
    /// - `HATCREEK`/`BIMA`, `IRAMPDB`/`IRAM30M`, `OVRO`, `GMRT`,
    ///   `NRAO12M`, `GBT`, `WSRT` and `ATCA`: their Gaussian, Airy,
    ///   numeric, cosine or band-dependent polynomial profiles.
    ///
    /// `None` for every other telescope, whose patterns casa-rs does not yet
    /// have.
    #[must_use]
    pub fn for_telescope(telescope: &str, reference_frequency_hz: f64) -> Option<Self> {
        if telescope.starts_with("ALMA") || telescope.starts_with("ATF") {
            Some(Self::ALMA)
        } else if telescope.starts_with("ACA") {
            Some(Self::ACA)
        } else if telescope.starts_with("VLA") {
            Some(match VlaBand::of(reference_frequency_hz) {
                Some(_) => Self::VLA,
                None => Self::VLA_NVSS,
            })
        } else if telescope.starts_with("EVLA") {
            Some(Self::Evla)
        } else if telescope.starts_with("NGVLA") {
            Some(Self::NGVLA)
        } else if telescope.starts_with("SMA") {
            Some(Self::Sma)
        } else if telescope.starts_with("ATA") {
            Some(Self::ATA)
        } else if telescope.starts_with("HATCREEK") || telescope.starts_with("BIMA") {
            Some(Self::Gaussian {
                half_width_arcmin_ghz: 191.67 / 2.,
                maximum_radius_arcmin_ghz: 215.,
                is_voltage: false,
            })
        } else if telescope.starts_with("IRAMPDB")
            || telescope.starts_with("IRAM30M")
            || (telescope.contains("IRAM") && telescope.contains("PDB"))
        {
            Some(Self::Airy {
                dish_diameter_m: 15.,
                blockage_diameter_m: 1.,
                maximum_radius_arcmin_ghz: 1.784 * 60.,
            })
        } else {
            DefaultRadial::for_telescope(telescope, reference_frequency_hz).map(Self::DefaultRadial)
        }
    }

    /// Whether CASA applies squint to this pattern. Its VLA band profiles
    /// can move RR/LL apart; the NVSS and EVLA implementations stay
    /// unsquinted, including when the caller requests `dosquint=True`.
    #[must_use]
    pub fn squints(self) -> bool {
        self == Self::VLA
    }

    /// The radius the pattern is tabulated to, in arcmin times GHz
    /// (`PBMath1D::maximumRadius_p`); the pattern is zero beyond it.
    #[must_use]
    pub const fn maximum_radius_arcmin_ghz(self) -> f64 {
        match self {
            Self::DefaultRadial(profile) => profile.maximum_radius(),
            Self::Sma => SmaVoltageTable::MAXIMUM_RADIUS,
            Self::Airy {
                maximum_radius_arcmin_ghz,
                ..
            } => maximum_radius_arcmin_ghz,
            Self::Evla => EvlaPowerPattern::MAXIMUM_RADIUS_ARCMIN_GHZ,
            Self::Gaussian {
                maximum_radius_arcmin_ghz,
                ..
            } => maximum_radius_arcmin_ghz,
        }
    }

    /// `PBMath1D::support`: the pattern's diameter in pixels of
    /// `increment_x_rad` at `frequency_hz`, rounded down, without
    /// tabulating it.
    #[must_use]
    pub fn support_pixels(self, increment_x_rad: f64, frequency_hz: f64) -> usize {
        support_pixels(
            self.maximum_radius_arcmin_ghz(),
            increment_x_rad,
            frequency_hz,
        )
    }

    /// The pattern's lookup: an Airy pattern's table, shared by every
    /// frequency it is evaluated at, or the EVLA's fits, which hold none.
    #[must_use]
    pub fn pattern(self) -> BeamPattern {
        BeamPattern(match self {
            Self::DefaultRadial(profile) => return profile.pattern(),
            Self::Sma => Lookup::Sma(SmaVoltageTable::new()),
            Self::Airy {
                dish_diameter_m,
                blockage_diameter_m,
                maximum_radius_arcmin_ghz,
            } => Lookup::Airy(AnnularApertureVoltageTable::new(
                dish_diameter_m,
                blockage_diameter_m,
                maximum_radius_arcmin_ghz,
            )),
            Self::Evla => Lookup::Evla,
            Self::Gaussian {
                half_width_arcmin_ghz,
                maximum_radius_arcmin_ghz,
                is_voltage,
            } => Lookup::Gaussian(GaussianVoltageTable::new(
                half_width_arcmin_ghz,
                maximum_radius_arcmin_ghz,
                is_voltage,
            )),
        })
    }
}

/// A primary beam's power pattern, as it is looked up by radius.
#[derive(Clone)]
pub struct BeamPattern(Lookup);

#[derive(Clone)]
enum Lookup {
    WideInverse(DefaultRadial),
    Tabulated(tabulated::VoltageTable),
    Sma(SmaVoltageTable),
    /// An Airy pattern's voltage table.
    Airy(AnnularApertureVoltageTable),
    /// The EVLA's fits, picked per frequency.
    Evla,
    Gaussian(GaussianVoltageTable),
}

impl BeamPattern {
    /// The most bytes a pattern holds: an Airy pattern's table.
    pub const BYTES: u64 = AnnularApertureVoltageTable::BYTES as u64 + 32;

    /// The pattern at `frequency_hz`: the Airy table, or the EVLA's fit of
    /// that frequency.
    #[must_use]
    pub fn at_frequency(&self, frequency_hz: f64) -> FrequencyPattern<'_> {
        FrequencyPattern(match &self.0 {
            Lookup::WideInverse(profile) => FrequencyLookup::WideInverse(profile.fit(frequency_hz)),
            Lookup::Tabulated(table) => FrequencyLookup::Tabulated(table),
            Lookup::Sma(table) => FrequencyLookup::Sma(table),
            Lookup::Airy(table) => FrequencyLookup::Airy(table),
            Lookup::Evla => FrequencyLookup::Evla(EvlaPowerPattern::at(frequency_hz)),
            Lookup::Gaussian(table) => FrequencyLookup::Gaussian(table),
        })
    }

    /// The power pattern at `separation_rad` from the pointing, at
    /// `frequency_hz`, as `PBMath1D::apply` evaluates it for a component:
    /// the separation in arcmin times the frequency in GHz, in `Double`,
    /// looked up in the table (the EVLA's fit of that frequency, as
    /// `PBMath1DEVLA` takes it per channel).
    #[must_use]
    pub fn power_at(&self, separation_rad: f64, frequency_hz: f64) -> f32 {
        let radius = separation_rad.to_degrees() * 60.0 * frequency_hz / 1.0e9;
        self.at_frequency(frequency_hz).power(radius)
    }

    /// The power pattern `offset_rad` along an image's x axis from its
    /// centre, at `frequency_hz`, with `PBMath1D::apply`'s arithmetic on an
    /// image (the offset in degrees kept as `Float`, the radius times 60 ·
    /// ν/GHz kept as `Float`), as `SDGrid::findPBAsConvFunction` applies the
    /// beam to a one-row image.
    #[must_use]
    pub fn power_on_axis(&self, offset_rad: f64, frequency_hz: f64) -> f32 {
        let ghz = frequency_hz / 1.0e9;
        let l_deg = offset_rad.to_degrees() as f32;
        let radius_deg = (l_deg * l_deg).sqrt();
        let radius = (f64::from(radius_deg) * 60.0 * ghz) as f32;
        self.at_frequency(frequency_hz).power(f64::from(radius))
    }

    /// `PBMath1D::support`: the pattern's diameter in pixels of
    /// `increment_x_rad` at `frequency_hz`, rounded down.
    #[must_use]
    pub fn support_pixels(&self, increment_x_rad: f64, frequency_hz: f64) -> usize {
        let maximum_arcmin_ghz = match &self.0 {
            Lookup::WideInverse(profile) => profile.maximum_radius(),
            Lookup::Tabulated(table) => table.maximum_radius,
            Lookup::Sma(_) => SmaVoltageTable::MAXIMUM_RADIUS,
            Lookup::Airy(table) => table.maximum_radius(),
            Lookup::Evla => EvlaPowerPattern::MAXIMUM_RADIUS_ARCMIN_GHZ,
            Lookup::Gaussian(table) => table.maximum_radius(),
        };
        support_pixels(maximum_arcmin_ghz, increment_x_rad, frequency_hz)
    }
}

/// `PBMath1D::support`: the diameter in pixels of `increment_x_rad` at
/// `frequency_hz` of a pattern tabulated to `maximum_arcmin_ghz`, rounded
/// down.
fn support_pixels(maximum_arcmin_ghz: f64, increment_x_rad: f64, frequency_hz: f64) -> usize {
    let radius_rad = (maximum_arcmin_ghz / 60.0).to_radians();
    (radius_rad / increment_x_rad.abs() * 2.0 * 1.0e9 / frequency_hz).floor() as usize
}

/// A pattern at one frequency ([`BeamPattern::at_frequency`]).
pub struct FrequencyPattern<'a>(FrequencyLookup<'a>);

enum FrequencyLookup<'a> {
    WideInverse(defaults::InverseFit),
    Tabulated(&'a tabulated::VoltageTable),
    Sma(&'a SmaVoltageTable),
    Airy(&'a AnnularApertureVoltageTable),
    Evla(EvlaPowerPattern),
    Gaussian(&'a GaussianVoltageTable),
}

impl FrequencyPattern<'_> {
    /// The power at `radius_arcmin_ghz`, the radius in arcmin times the
    /// frequency in GHz, and zero beyond the table (`PBMath1D`'s lookup;
    /// `BeamSkyJones::apply` squares the voltage pattern).
    #[must_use]
    pub fn power(&self, radius_arcmin_ghz: f64) -> f32 {
        let voltage = self.voltage(radius_arcmin_ghz);
        voltage * voltage
    }

    /// The voltage at `radius_arcmin_ghz`, the radius in arcmin times the
    /// frequency in GHz, and zero beyond the table: the `vp_p` sample
    /// `PBMath1D::apply` multiplies an image by for a voltage pattern
    /// (`applyVP`), and squares for a power pattern.
    #[must_use]
    pub fn voltage(&self, radius_arcmin_ghz: f64) -> f32 {
        match &self.0 {
            FrequencyLookup::WideInverse(fit) => fit.voltage(radius_arcmin_ghz),
            FrequencyLookup::Tabulated(table) => table.evaluate(radius_arcmin_ghz),
            FrequencyLookup::Sma(table) => table.evaluate(radius_arcmin_ghz),
            FrequencyLookup::Airy(table) => table.evaluate(radius_arcmin_ghz),
            FrequencyLookup::Evla(fit) => fit.voltage(radius_arcmin_ghz),
            FrequencyLookup::Gaussian(table) => table.evaluate(radius_arcmin_ghz),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telescopes_match_casas_common_beams_by_prefix_and_band() {
        let beam = |telescope: &str, ghz: f64| PrimaryBeam::for_telescope(telescope, ghz * 1.0e9);
        assert_eq!(beam("ALMA", 230.0), Some(PrimaryBeam::ALMA));
        assert_eq!(beam("ALMASD", 100.0), Some(PrimaryBeam::ALMA));
        assert_eq!(beam("ATF", 230.0), Some(PrimaryBeam::ALMA));
        assert_eq!(beam("ACA", 230.0), Some(PrimaryBeam::ACA));
        assert_eq!(beam("ACA-7m", 230.0), Some(PrimaryBeam::ACA));
        // The VLA's pattern follows spectral window 0's band: between CASA's
        // bands (3 GHz) it is the NVSS pattern.
        assert_eq!(beam("VLA", 1.4), Some(PrimaryBeam::VLA));
        assert_eq!(beam("VLA", 45.0), Some(PrimaryBeam::VLA));
        assert_eq!(beam("VLA", 3.0), Some(PrimaryBeam::VLA_NVSS));
        assert_eq!(beam("EVLA", 1.4), Some(PrimaryBeam::Evla));
        assert_eq!(beam("EVLA", 18.5), Some(PrimaryBeam::Evla));
        for other in ["alma", "Unknown", "", "AL"] {
            assert_eq!(beam(other, 1.4), None, "{other}");
        }
    }
}
