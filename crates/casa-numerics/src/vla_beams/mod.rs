// SPDX-License-Identifier: LGPL-3.0-or-later
//! The VLA's and the EVLA's primary beams by frequency, as CASA's `PBMath`
//! gives them (`synthesis/TransformMachines`):
//! - [`VlaBand`]: the band a frequency selects among the VLA's common beams
//!   (`PBMath::whichCommonPBtoUse`), each an annular-aperture pattern
//!   ([`crate::AnnularApertureVoltageTable`]);
//! - [`EvlaBand`] and [`EvlaPowerPattern`]: the EVLA's polynomial power
//!   patterns (`PBMath1DEVLA`, `PBMath1DPoly`), one fit per tabulated
//!   frequency.

mod evla_coefficients;

use evla_coefficients::EVLA_COEFFICIENTS;

/// A band of the VLA's common beams (`PBMath::whichCommonPBtoUse`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VlaBand {
    /// `VLA_4`, below 0.1 GHz.
    FourMetre,
    /// `VLA_P`, 0.2–0.4 GHz.
    P,
    /// `VLA_L`, 1–2 GHz.
    L,
    /// `VLA_C`, 4–7 GHz.
    C,
    /// `VLA_X`, 7–11 GHz.
    X,
    /// `VLA_U`, 11–19 GHz.
    U,
    /// `VLA_K`, 19–35 GHz.
    K,
    /// `VLA_Q`, 35–55 GHz.
    Q,
}

impl VlaBand {
    /// The band of `frequency_hz` in CASA's open intervals, tested from Q
    /// down, or `None` outside them, where CASA takes its `VLA_NVSS`
    /// pattern.
    #[must_use]
    pub fn of(frequency_hz: f64) -> Option<Self> {
        let ghz = frequency_hz / 1.0e9;
        let within = |low: f64, high: f64| ghz > low && ghz < high;
        [
            (35.0, 55.0, Self::Q),
            (19.0, 35.0, Self::K),
            (11.0, 19.0, Self::U),
            (7.0, 11.0, Self::X),
            (4.0, 7.0, Self::C),
            (1.0, 2.0, Self::L),
            (0.2, 0.4, Self::P),
        ]
        .into_iter()
        .find_map(|(low, high, band)| within(low, high).then_some(band))
        .or((ghz < 0.1).then_some(Self::FourMetre))
    }
}

/// A band of the EVLA's polynomial beams (`PBMath1DEVLA::feed`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvlaBand {
    /// P, 224–480 MHz.
    P,
    /// L, 900–2003 MHz.
    L,
    /// S, 1990–4001 MHz.
    S,
    /// C, 3990–8001 MHz.
    C,
    /// X, 7990–12001 MHz.
    X,
    /// U (Ku), 12000–18000 MHz.
    U,
    /// K, 19000–26000 MHz.
    K,
    /// A (Ka), 28000–38000 MHz.
    A,
    /// Q, 41000–50000 MHz.
    Q,
}

impl EvlaBand {
    /// The band of `frequency_hz`: the first of CASA's open intervals, in
    /// its order, that holds it (the intervals overlap at their edges), or
    /// `None` between them.
    #[must_use]
    pub fn of(frequency_hz: f64) -> Option<Self> {
        let mhz = frequency_hz / 1.0e6;
        [
            (224.0, 480.0, Self::P),
            (900.0, 2003.0, Self::L),
            (1990.0, 4001.0, Self::S),
            (3990.0, 8001.0, Self::C),
            (7990.0, 12001.0, Self::X),
            (12000.0, 18000.0, Self::U),
            (19000.0, 26000.0, Self::K),
            (28000.0, 38000.0, Self::A),
            (41000.0, 50000.0, Self::Q),
        ]
        .into_iter()
        .find_map(|(low, high, band)| (mhz > low && mhz < high).then_some(band))
    }

    /// `frequency_hz` kept within the band's fits
    /// (`PBMath1DEVLA::limitFreqForBand`).
    fn limit(self, frequency_hz: f64) -> f64 {
        // (whether the low edge itself is limited, the low edge, its limit,
        // the high edge, its limit), in MHz: P and L limit at or below their
        // low edge, the others below it; every band at or above its high
        // edge.
        let (inclusive_low, low, low_limit, high, high_limit) = match self {
            Self::P => (true, 224.0, 232.0, 480.0, 470.0),
            Self::L => (true, 900.0, 1040.0, 2000.0, 2000.0),
            Self::S => (false, 2052.0, 2052.0, 3948.0, 3948.0),
            Self::C => (false, 4052.0, 4052.0, 7948.0, 7948.0),
            Self::X => (false, 8052.0, 8052.0, 11948.0, 11948.0),
            Self::U => (false, 12052.0, 12052.0, 17948.0, 17948.0),
            Self::K => (false, 19052.0, 19052.0, 25948.0, 25948.0),
            Self::A => (false, 28052.0, 28052.0, 38048.0, 38048.0),
            Self::Q => (false, 41052.0, 41052.0, 43948.0, 43948.0),
        };
        let mut frequency_hz = frequency_hz;
        let below = if inclusive_low {
            frequency_hz <= low * 1.0e6
        } else {
            frequency_hz < low * 1.0e6
        };
        if below {
            frequency_hz = low_limit * 1.0e6;
        }
        if frequency_hz >= high * 1.0e6 {
            frequency_hz = high_limit * 1.0e6;
        }
        frequency_hz
    }
}

/// One of the EVLA's polynomial power patterns (`PBMath1DPoly`), as
/// `PBMath1DEVLA::nearestVPArray` picks it for a frequency.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EvlaPowerPattern {
    frequency_mhz: f64,
    coefficients: &'static [f64],
}

impl EvlaPowerPattern {
    /// The radius the pattern is tabulated to, in arcmin at 1 GHz
    /// (`PBMath::initByTelescope`: 58′).
    pub const MAXIMUM_RADIUS_ARCMIN_GHZ: f64 = 58.0;

    /// CASA's table samples (`PBMath1DPoly::fillPBArray`).
    const SAMPLES: f64 = 10_000.0;

    /// The pattern CASA applies at `frequency_hz`
    /// (`PBMath1DEVLA::nearestVPArray`): the frequency is kept within its
    /// band's fits ([`EvlaBand::of`], `limitFreqForBand`; a frequency
    /// between bands is kept as it is), and the fit of the nearest tabulated
    /// frequency, the higher on a tie, is taken from the whole table.
    #[must_use]
    pub fn at(frequency_hz: f64) -> Self {
        let frequency_hz =
            EvlaBand::of(frequency_hz).map_or(frequency_hz, |band| band.limit(frequency_hz));
        let mhz = frequency_hz * 1.0e-6;
        // `std::map::lower_bound`: the first fit at or above the frequency.
        let upper = EVLA_COEFFICIENTS.partition_point(|(fit, _)| *fit < mhz);
        let index = if upper == EVLA_COEFFICIENTS.len() {
            upper - 1
        } else if upper == 0 {
            0
        } else if (mhz - EVLA_COEFFICIENTS[upper - 1].0).abs()
            < (EVLA_COEFFICIENTS[upper].0 - mhz).abs()
        {
            upper - 1
        } else {
            upper
        };
        let (frequency_mhz, coefficients) = EVLA_COEFFICIENTS[index];
        Self {
            frequency_mhz,
            coefficients,
        }
    }

    /// The tabulated frequency of the fit, in MHz.
    #[must_use]
    pub const fn frequency_mhz(&self) -> f64 {
        self.frequency_mhz
    }

    /// The fit's even-power coefficients: the power is `Σ c_k x^k`, with
    /// `x` the square of the radius in arcmin times the frequency in GHz.
    #[must_use]
    pub const fn coefficients(&self) -> &'static [f64] {
        self.coefficients
    }

    /// The power at `radius_arcmin_ghz`, the radius in arcmin times the
    /// frequency in GHz. As CASA tabulates the pattern
    /// (`PBMath1DPoly::fillPBArray`, `PBMath1D::apply`): 10,000 samples to
    /// [`Self::MAXIMUM_RADIUS_ARCMIN_GHZ`], the lower sample taken, the
    /// voltage stored as `Float` (the square root of the power) and
    /// squared. Zero beyond the table and where the polynomial is not
    /// positive.
    #[must_use]
    pub fn power(&self, radius_arcmin_ghz: f64) -> f32 {
        if !(radius_arcmin_ghz.is_finite()
            && (0.0..=Self::MAXIMUM_RADIUS_ARCMIN_GHZ).contains(&radius_arcmin_ghz))
        {
            return 0.0;
        }
        let inverse_increment = (Self::SAMPLES - 1.0) / Self::MAXIMUM_RADIUS_ARCMIN_GHZ;
        let sampled = (radius_arcmin_ghz * inverse_increment).floor() / inverse_increment;
        let x2 = sampled * sampled;
        let mut response = 0.0;
        let mut power = 1.0;
        for coefficient in self.coefficients {
            response += coefficient * power;
            power *= x2;
        }
        if response <= 0.0 {
            0.0
        } else {
            let voltage = response.sqrt() as f32;
            voltage * voltage
        }
    }
}

#[cfg(test)]
mod tests;
