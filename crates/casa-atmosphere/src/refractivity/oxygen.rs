// SPDX-License-Identifier: LGPL-3.0-or-later
// Ported from ATM's ATMRefractiveIndex.cc, (c) Instituto de Estructura de la
// Materia, Pardo et al., LGPL-2.1-or-later.
//! O2 line refractivities (ATM's opacity sources 1 to 4).
//!
//! The (16)O(16)O ground-state lines (the 60 GHz band and 118.75 GHz) are
//! broadened by dry air and water vapour and include line interference.
//! ATM adds nine weak mesospheric O2 (¹Δ) lines to them. The vibrational
//! state and the isotopologues use a fixed broadening.

use super::tables::{o2, o2_16o18o, o2_vib};
use super::{
    H_OVER_K, PI, PICUBE8DIV3HCESU, ZERO, by_pressure, frequency_bin, interference_o2,
    line_broadening, line_broadening_o2, line_shape, window_bounds, window_range,
};
use num_complex::Complex64;

/// O2's dipole moment as ATM writes it (the magnetic dipole, in its units).
const MU: f64 = 0.0186;

/// (16)O(16)O from the ground vibrational state (source 1), up to
/// 1599.9 GHz.
///
/// The nine mesospheric lines are multiplied by 3e13 in layers below
/// 1.7 hPa, and they count only when the main catalogue's window is not
/// empty. Between 118.750345 and 124 GHz the window starts at line 37
/// (the 118.75 GHz line). A negative absorption is set to zero, keeping
/// the delay.
pub(super) fn o2(tt: f64, pp: f64, eh2o: f64, nu: f64) -> Complex64 {
    if nu > 1599.9 {
        return ZERO;
    }
    let mut sum = ZERO;
    for i in 0..o2::FREEXTRA.len() {
        let fre = o2::FREEXTRA[i];
        let width = line_broadening(
            fre,
            tt,
            pp,
            32.0,
            o2::BRDO2AIREXTRA[i] * 0.001,
            o2::TEXPO2EXTRA[i],
        );
        let mut shape = line_shape(nu, fre, width, 0.0)
            * o2::FLINEXTRA[i]
            * (-(o2::EUEXTRA[i] - 0.0479924207 * fre) / tt).exp()
            * fre;
        if pp < 1.7 {
            shape *= 30000000000000.0;
        }
        sum += shape;
    }
    let tables = by_pressure(
        pp,
        (&o2::INI1[..], &o2::IFIN1[..]),
        (&o2::INI1[..], &o2::IFIN1[..]),
        (&o2::INI2[..], &o2::IFIN2[..]),
    );
    let (mut start, end) = window_bounds(frequency_bin(nu), 2, tables, 0);
    if (118.750345..=124.0).contains(&nu) {
        start = 37;
    }
    let Some(range) = window_range(start, end) else {
        return ZERO;
    };
    for i in range {
        let fre = o2::FRE[i];
        let [b1, b2, b3, b4] = o2::BROADENING[i];
        let width = line_broadening_o2(fre, tt, pp, eh2o, 32.0, b1, b2);
        sum += line_shape(nu, fre, width, interference_o2(tt, pp, b3, b4))
            * o2::FLIN[i]
            * (-o2::EL[i] / tt).exp()
            * (1.0 - (-H_OVER_K * fre / tt).exp());
    }
    let mut sum = sum * (nu / PI) * (PICUBE8DIV3HCESU * MU.powf(2.0) / (0.72923 * tt));
    if sum.im < 0.0 {
        sum = Complex64::new(sum.re, 0.0);
    }
    sum * 1e-4
}

/// A fixed-broadening O2 catalogue summed over all its lines, up to
/// 999.9 GHz.
fn fixed_broadening(
    tt: f64,
    pp: f64,
    eh2o: f64,
    nu: f64,
    mmol: f64,
    q: f64,
    lines: [&[f64]; 3],
) -> Complex64 {
    if nu > 999.9 {
        return ZERO;
    }
    let [frequency, intensity, energy] = lines;
    let mut sum = ZERO;
    for i in 0..frequency.len() {
        let fre = frequency[i];
        let width = line_broadening_o2(fre, tt, pp, eh2o, mmol, 2.0, 0.2);
        sum += line_shape(nu, fre, width, 0.0)
            * intensity[i]
            * (-energy[i] / tt).exp()
            * (1.0 - (-H_OVER_K * fre / tt).exp());
    }
    sum * (nu / PI) * (PICUBE8DIV3HCESU * MU.powf(2.0) / q) * 1e-4
}

/// (16)O(16)O from the first vibrational state (source 2).
pub(super) fn o2_vibrational(tt: f64, pp: f64, eh2o: f64, nu: f64) -> Complex64 {
    fixed_broadening(
        tt,
        pp,
        eh2o,
        nu,
        32.0,
        0.72923 * tt,
        [&o2_vib::FRE, &o2_vib::FLIN, &o2_vib::EL],
    )
}

/// (16)O(18)O (source 3).
pub(super) fn o2_isotope18(tt: f64, pp: f64, eh2o: f64, nu: f64) -> Complex64 {
    fixed_broadening(
        tt,
        pp,
        eh2o,
        nu,
        34.0,
        1.536568889 * tt,
        [&o2_16o18o::FRE, &o2_16o18o::FLIN, &o2_16o18o::EL],
    )
}

/// (16)O(17)O (source 4). ATM returns zero before summing its catalogue
/// (`tables::o2_16o17o`), so this species contributes nothing.
pub(super) fn o2_isotope17(_nu: f64) -> Complex64 {
    ZERO
}
