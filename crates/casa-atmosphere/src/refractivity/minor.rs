// SPDX-License-Identifier: LGPL-3.0-or-later
// Ported from ATM's ATMRefractiveIndex.cc, (c) Instituto de Estructura de la
// Materia, Pardo et al., LGPL-2.1-or-later.
//! The minor gases' line refractivities: SO2, HCl, HCN, NO2, N2O and CO
//! (ATM's opacity sources 8, 24, 25, 7, 6 and 5).
//!
//! Each is a sum of air-broadened lines weighted by intensity, Boltzmann
//! factor and line frequency, scaled by (ν/π)·((h/k)/T)·(8π³/3hc)μ²/Q.

use super::tables::{co, hcl, hcn, n2o, no2, so2};
use super::{
    AirLines, Exponent, H_OVER_K, PI, PICUBE8DIV3HCESU, ZERO, by_pressure, frequency_bin, window,
};
use num_complex::Complex64;

/// The factor ATM applies to a minor gas's line sum, in its order:
/// ν/π, then (h/k)/T, then (8π³/3hc)μ²/Q, then 1e-4 (cm² to m²).
fn scaled(sum: Complex64, nu: f64, tt: f64, mu: f64, q: f64) -> Complex64 {
    sum * (nu / PI) * (H_OVER_K / tt) * (PICUBE8DIV3HCESU * mu.powf(2.0) / q) * 1e-4
}

/// SO2 (source 8). ATM has no SO2 lines above 499.99 GHz.
pub(super) fn so2(tt: f64, pp: f64, nu: f64) -> Complex64 {
    if nu > 499.99 {
        return ZERO;
    }
    let lines = AirLines {
        frequency: &so2::FRE,
        intensity: &so2::FLIN,
        energy: &so2::EL,
        broadening: &so2::BRDSO2AIR,
        exponent: Exponent::Fixed(0.75),
    };
    let Some(range) = window(frequency_bin(nu), 2, (&so2::INI1, &so2::IFIN1)) else {
        return ZERO;
    };
    let q = 1.1346738633 * tt * tt.sqrt();
    scaled(lines.sum(nu, tt, pp, 64.0644, range), nu, tt, 1.62, q)
}

/// HCl (source 24): three hyperfine lines near 625.9 GHz, summed only
/// between 624 and 627.8 GHz.
pub(super) fn hcl(tt: f64, pp: f64, nu: f64) -> Complex64 {
    if !(nu > 624.0 && nu < 627.8) {
        return ZERO;
    }
    let lines = AirLines {
        frequency: &hcl::FRE,
        intensity: &hcl::FLIN,
        energy: &hcl::EL,
        broadening: &hcl::BRDHCLAIR,
        exponent: Exponent::PerLine(&hcl::TEXPHCL),
    };
    let q = 0.0668793372 * tt;
    scaled(lines.sum(nu, tt, pp, 36.0, 0..=2), nu, tt, 0.816, q)
}

/// HCN (source 25): all 18 lines at every frequency.
pub(super) fn hcn(tt: f64, pp: f64, nu: f64) -> Complex64 {
    let lines = AirLines {
        frequency: &hcn::FRE,
        intensity: &hcn::FLIN,
        energy: &hcn::EL,
        broadening: &hcn::BRDHCNAIR,
        exponent: Exponent::PerLine(&hcn::TEXPHCN),
    };
    let q = 0.470182585 * tt;
    scaled(lines.sum(nu, tt, pp, 27.0, 0..=17), nu, tt, 2.98, q)
}

/// NO2 (source 7). ATM has no NO2 lines above 999.9 GHz.
pub(super) fn no2(tt: f64, pp: f64, nu: f64) -> Complex64 {
    let vp = frequency_bin(nu);
    if nu > 999.9 || (nu > 20.0 && vp == 0) {
        return ZERO;
    }
    let lines = AirLines {
        frequency: &no2::FRE,
        intensity: &no2::FLIN,
        energy: &no2::EL,
        broadening: &no2::BRDNO2AIR,
        exponent: Exponent::PerLine(&no2::TEXPNO2),
    };
    let Some(range) = window(vp, 2, (&no2::INI1, &no2::IFIN1)) else {
        return ZERO;
    };
    let q = 0.430965924 * tt * tt.sqrt();
    scaled(lines.sum(nu, tt, pp, 46.0, range), nu, tt, 0.3161, q)
}

/// N2O (source 6). ATM has no N2O lines above 999.9 GHz.
pub(super) fn n2o(tt: f64, pp: f64, nu: f64) -> Complex64 {
    if nu > 999.9 {
        return ZERO;
    }
    let lines = AirLines {
        frequency: &n2o::FRE,
        intensity: &n2o::FLIN,
        energy: &n2o::EL,
        broadening: &n2o::BRDN2OAIR,
        exponent: Exponent::PerLine(&n2o::TEXPN2O),
    };
    let tables = by_pressure(
        pp,
        (&n2o::INI1[..], &n2o::IFIN1[..]),
        (&n2o::INI2[..], &n2o::IFIN2[..]),
        (&n2o::INI3[..], &n2o::IFIN3[..]),
    );
    let Some(range) = window(frequency_bin(nu), 2, tables) else {
        return ZERO;
    };
    let q = 1.65873970 * tt;
    scaled(lines.sum(nu, tt, pp, 44.0, range), nu, tt, 0.161, q)
}

/// CO (source 5). ATM has no CO lines above 999.9 GHz.
pub(super) fn co(tt: f64, pp: f64, nu: f64) -> Complex64 {
    if nu > 999.9 {
        return ZERO;
    }
    let lines = AirLines {
        frequency: &co::FRE,
        intensity: &co::FLIN,
        energy: &co::EL,
        broadening: &co::BRDCOAIR,
        exponent: Exponent::PerLine(&co::TEXPCO),
    };
    let tables = by_pressure(
        pp,
        (&co::INI1[..], &co::IFIN1[..]),
        (&co::INI2[..], &co::IFIN2[..]),
        (&co::INI3[..], &co::IFIN3[..]),
    );
    let Some(range) = window(frequency_bin(nu), 2, tables) else {
        return ZERO;
    };
    let q = 0.3615187262 * tt;
    scaled(lines.sum(nu, tt, pp, 28.0, range), nu, tt, 0.112, q)
}
