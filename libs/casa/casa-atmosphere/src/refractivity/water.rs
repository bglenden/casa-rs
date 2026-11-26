// SPDX-License-Identifier: LGPL-3.0-or-later
// Ported from ATM's ATMRefractiveIndex.cc, (c) Instituto de Estructura de la
// Materia, Pardo et al., LGPL-2.1-or-later.
//! Water-vapour line refractivities and the two continua (ATM's opacity
//! sources 9 to 15).
//!
//! The water lines carry ATM's spin-statistics weights `gl` and the
//! stimulated-emission factor 1 − exp(−hνₗ/kT), and use water's own
//! broadening (air and self). The continua are ATM's empirical
//! collision-induced terms, already per unit path.

use super::tables::{hdo, hh16o, hh16o_v2, hh17o, hh18o};
use super::{
    H_OVER_K, PI, PICUBE8DIV3HCESU, WindowTables, ZERO, by_pressure, frequency_bin,
    line_broadening_rare_water, line_broadening_water, line_shape, window, window_bounds,
    window_range,
};
use num_complex::Complex64;
use std::ops::RangeInclusive;

/// A water catalogue broadened by ATM's four tabulated air/self
/// coefficients.
struct WaterLines<'a> {
    frequency: &'a [f64],
    intensity: &'a [f64],
    energy: &'a [f64],
    weight: &'a [f64],
    broadening: &'a [[f64; 4]],
}

impl WaterLines<'_> {
    fn sum(&self, nu: f64, tt: f64, pp: f64, eh2o: f64, lines: RangeInclusive<usize>) -> Complex64 {
        let mut sum = ZERO;
        for i in lines {
            let [b1, b2, b3, b4] = self.broadening[i];
            let fre = self.frequency[i];
            let width = line_broadening_water(fre, tt, pp, eh2o, b1, b2, b3, b4);
            sum += line_shape(nu, fre, width, 0.0)
                * self.intensity[i]
                * self.weight[i]
                * (-self.energy[i] / tt).exp()
                * (1.0 - (-H_OVER_K * fre / tt).exp());
        }
        sum
    }
}

/// ATM's factor for the water species, in its order.
fn scaled(sum: Complex64, nu: f64, mu: f64, q: f64) -> Complex64 {
    sum * (nu / PI) * (PICUBE8DIV3HCESU * mu.powf(2.0) / q) * 1e-4
}

/// The three pressure-dependent window tables of a catalogue.
fn pressure_tables<'a>(
    pp: f64,
    high: (&'a [u16], &'a [u16]),
    mid: (&'a [u16], &'a [u16]),
    low: (&'a [u16], &'a [u16]),
) -> WindowTables<'a> {
    by_pressure(pp, high, mid, low)
}

/// H2(16)O from the ground vibrational state (source 11). Above
/// 1595.9 GHz ATM sums the whole catalogue.
pub(super) fn h2o(tt: f64, pp: f64, eh2o: f64, nu: f64) -> Complex64 {
    let range = if nu > 1595.9 {
        Some(0..=hh16o::FRE.len() - 1)
    } else {
        let tables = pressure_tables(
            pp,
            (&hh16o::INI1, &hh16o::IFIN1),
            (&hh16o::INI2, &hh16o::IFIN2),
            (&hh16o::INI3, &hh16o::IFIN3),
        );
        window(frequency_bin(nu), 2, tables)
    };
    let Some(range) = range else {
        return ZERO;
    };
    let lines = WaterLines {
        frequency: &hh16o::FRE,
        intensity: &hh16o::FLIN,
        energy: &hh16o::EL,
        weight: &hh16o::GL,
        broadening: &hh16o::BROADENING,
    };
    scaled(
        lines.sum(nu, tt, pp, eh2o, range),
        nu,
        1.855,
        0.034278209 * tt * tt.sqrt(),
    )
}

/// H2(16)O from the first bending state (source 12), up to 999.9 GHz,
/// with windows 60 bins either side.
pub(super) fn h2o_bending(tt: f64, pp: f64, eh2o: f64, nu: f64) -> Complex64 {
    if nu > 999.9 {
        return ZERO;
    }
    let tables = pressure_tables(
        pp,
        (&hh16o_v2::INI1, &hh16o_v2::IFIN1),
        (&hh16o_v2::INI2, &hh16o_v2::IFIN2),
        (&hh16o_v2::INI3, &hh16o_v2::IFIN3),
    );
    let Some(range) = window(frequency_bin(nu), 60, tables) else {
        return ZERO;
    };
    let lines = WaterLines {
        frequency: &hh16o_v2::FRE,
        intensity: &hh16o_v2::FLIN,
        energy: &hh16o_v2::EL,
        weight: &hh16o_v2::GL,
        broadening: &hh16o_v2::BROADENING,
    };
    scaled(
        lines.sum(nu, tt, pp, eh2o, range),
        nu,
        1.82332,
        0.034256116 * tt * tt.sqrt(),
    )
}

/// A rare water isotopologue's catalogue, broadened by ATM's
/// `linebroadening_hh18o_hh17o`.
struct RareWaterLines<'a> {
    frequency: &'a [f64],
    intensity: &'a [f64],
    energy: &'a [f64],
    weight: &'a [f64],
    dv0: &'a [f64],
    dvlm: &'a [f64],
    exponent: &'a [f64],
}

impl RareWaterLines<'_> {
    fn sum(&self, nu: f64, tt: f64, pp: f64, eh2o: f64, lines: RangeInclusive<usize>) -> Complex64 {
        let mut sum = ZERO;
        for i in lines {
            let fre = self.frequency[i];
            let width = line_broadening_rare_water(
                tt,
                pp,
                eh2o,
                self.dv0[i],
                self.dvlm[i],
                self.exponent[i],
            );
            sum += line_shape(nu, fre, width, 0.0)
                * self.intensity[i]
                * self.weight[i]
                * (-self.energy[i] / tt).exp()
                * (1.0 - (-H_OVER_K * fre / tt).exp());
        }
        sum
    }
}

/// H2(18)O (source 13), up to 999.9 GHz, with windows 60 bins either side
/// and no pressure dependence.
pub(super) fn h2o_isotope18(tt: f64, pp: f64, eh2o: f64, nu: f64) -> Complex64 {
    if nu > 999.9 {
        return ZERO;
    }
    let Some(range) = window(frequency_bin(nu), 60, (&hh18o::INI1, &hh18o::IFIN1)) else {
        return ZERO;
    };
    let lines = RareWaterLines {
        frequency: &hh18o::FRE,
        intensity: &hh18o::FLIN,
        energy: &hh18o::EL,
        weight: &hh18o::GL,
        dv0: &hh18o::DV0,
        dvlm: &hh18o::DVLM,
        exponent: &hh18o::TEMP_EXP,
    };
    scaled(
        lines.sum(nu, tt, pp, eh2o, range),
        nu,
        1.855,
        0.034571542 * tt * tt.sqrt(),
    )
}

/// H2(17)O (source 14), up to 999.9 GHz, with pressure-dependent windows
/// 60 bins either side.
pub(super) fn h2o_isotope17(tt: f64, pp: f64, eh2o: f64, nu: f64) -> Complex64 {
    if nu > 999.9 {
        return ZERO;
    }
    let tables = pressure_tables(
        pp,
        (&hh17o::INI1, &hh17o::IFIN1),
        (&hh17o::INI2, &hh17o::IFIN2),
        (&hh17o::INI3, &hh17o::IFIN3),
    );
    let Some(range) = window(frequency_bin(nu), 60, tables) else {
        return ZERO;
    };
    let lines = RareWaterLines {
        frequency: &hh17o::FRE,
        intensity: &hh17o::FLIN,
        energy: &hh17o::EL,
        weight: &hh17o::GL,
        dv0: &hh17o::DV0,
        dvlm: &hh17o::DVLM,
        exponent: &hh17o::TEMP_EXP,
    };
    scaled(
        lines.sum(nu, tt, pp, eh2o, range),
        nu,
        1.855,
        0.034412578 * tt * tt.sqrt(),
    )
}

/// HDO (source 15), up to 999.9 GHz. ATM sums two blocks of its catalogue
/// with different dipole components: lines 0–21 (μa, broadened as
/// 0.003·P·(300/T)^0.7) and lines from 22 (μb, broadened as 0.003·P), and
/// quarters the total.
pub(super) fn hdo(tt: f64, pp: f64, nu: f64) -> Complex64 {
    const MUA: f64 = 0.657;
    const MUB: f64 = 1.732;
    if nu > 999.9 {
        return ZERO;
    }
    let vp = frequency_bin(nu);
    let first = pressure_tables(
        pp,
        (&hdo::INI11, &hdo::IFIN11),
        (&hdo::INI21, &hdo::IFIN21),
        (&hdo::INI31, &hdo::IFIN31),
    );
    let second = pressure_tables(
        pp,
        (&hdo::INI12, &hdo::IFIN12),
        (&hdo::INI22, &hdo::IFIN22),
        (&hdo::INI32, &hdo::IFIN32),
    );
    let q = 0.028263028 * tt * tt.sqrt();
    let block = |range: Option<RangeInclusive<usize>>, width: f64, mu: f64| {
        let Some(range) = range else {
            return ZERO;
        };
        let mut sum = ZERO;
        for i in range {
            sum += line_shape(nu, hdo::FRE[i], width, 0.0)
                * hdo::FLIN[i]
                * (-hdo::EL[i] / tt).exp()
                * (1.0 - (-H_OVER_K * hdo::FRE[i] / tt).exp());
        }
        sum * (nu / PI) * (PICUBE8DIV3HCESU * mu.powf(2.0) / q)
    };
    let (start, end) = window_bounds(vp, 60, first, 0);
    let a = block(
        window_range(start, end),
        0.003 * pp * (300.0 / tt).powf(0.7),
        MUA,
    );
    let (start, end) = window_bounds(vp, 60, second, 22);
    let b = block(window_range(start, end), 0.003 * pp, MUB);
    (a + b) * 1e-4 * 0.25
}

/// The water-vapour continuum (source 9), per unit path: ATM's empirical
/// ν² (ν^1.8 above 900 GHz) absorption and its delay term.
pub(super) fn continuum(tt: f64, pp: f64, eh2o: f64, nu: f64) -> Complex64 {
    let t300 = 300.0 / tt;
    let eee = eh2o / 1013.0;
    let ppp = (pp - eh2o) / 1013.0;
    let absorption = if nu < 900.0 {
        0.0315 * (nu / 225.0).powf(2.0) * (eee * ppp) * t300.powf(3.0)
    } else {
        0.0315 * (nu / 225.0).powf(1.8) * (eee * ppp) * t300.powf(3.0)
            - 0.0315 * (900.0_f64 / 225.0).powf(1.8) * (eee * ppp) * t300.powf(3.0)
            + 0.0315 * (900.0_f64 / 225.0).powf(2.0) * (eee * ppp) * t300.powf(3.0)
    };
    let delay = (4.163 * t300 + 0.239) * eh2o * t300 * nu * 1.2008e-3 / 57.29578;
    Complex64::new(delay, absorption)
}

/// The dry-air continuum (source 10), per unit path: ATM's O2 Debye and
/// N2 collision-induced absorption and the dry delay.
pub(super) fn dry_continuum(tt: f64, pp: f64, eh2o: f64, nu: f64) -> Complex64 {
    let t300 = 300.0 / tt;
    let ppp = pp - eh2o;
    let gamma0 = 5.6e-04 * pp * t300.powf(0.8);
    let nsec1 = 6.14e-5 * ppp * t300.powf(2.0) * nu * gamma0 / (nu.powf(2.0) + gamma0.powf(2.0));
    let nsec2 = 1.4e-12 * ppp.powf(2.0) * t300.powf(3.5) * nu;
    let absorption = (nsec1 + 0.85633 * nsec2) * nu * 0.1820 / 4.34294482e3;
    let delay = (0.2588 * ppp * t300
        - 6.14e-5 * ppp * t300.powf(2.0) * nu.powf(2.0) / (nu.powf(2.0) + gamma0.powf(2.0)))
        * nu
        * 1.2008e-3
        / 57.29578;
    Complex64::new(delay, absorption)
}
