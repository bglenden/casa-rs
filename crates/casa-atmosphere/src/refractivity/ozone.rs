// SPDX-License-Identifier: LGPL-3.0-or-later
// Ported from ATM's ATMRefractiveIndex.cc, (c) Instituto de Estructura de la
// Materia, Pardo et al., LGPL-2.1-or-later.
//! Ozone line refractivities: (16)O3 in its ground, ν1, ν2 and ν3 states
//! and the four singly substituted isotopologues (ATM's opacity sources 16
//! to 23).
//!
//! All are air-broadened line sums for molecular mass 48 (main isotope),
//! 49 or 50, with dipole moment 0.53 D. The (16)O3 states apply the
//! (h/k)/T factor after the strength factor; the isotopologues before it,
//! as ATM does.

use super::tables::{
    o3, o3_16o16o17o, o3_16o16o18o, o3_16o17o16o, o3_16o18o16o, o3_v1, o3_v2, o3_v3,
};
use super::{
    AirLines, Exponent, H_OVER_K, PI, PICUBE8DIV3HCESU, WindowTables, ZERO, by_pressure, window,
    window_range,
};
use num_complex::Complex64;

const MU: f64 = 0.53;

/// ATM's factor for the (16)O3 states, in its order.
fn main_scaled(sum: Complex64, nu: f64, tt: f64, q: f64) -> Complex64 {
    sum * (nu / PI) * (PICUBE8DIV3HCESU * MU.powf(2.0) / q) * (H_OVER_K / tt) * 1e-4
}

/// ATM's factor for the isotopologues, in its order.
fn isotope_scaled(sum: Complex64, nu: f64, tt: f64, q: f64) -> Complex64 {
    sum * (nu / PI) * (H_OVER_K / tt) * (PICUBE8DIV3HCESU * MU.powf(2.0) / q) * 1e-4
}

/// (16)O3 in the ground state (source 16), below 1599.999 GHz.
///
/// Unlike every other catalogue, ATM takes this one's window table entries
/// as 0-based line indices at the frequency bin itself (no `− 1` and no
/// neighbouring bins).
pub(super) fn o3(tt: f64, pp: f64, nu: f64, vp: usize) -> Complex64 {
    let mut sum = ZERO;
    if nu < 1599.999 {
        let (starts, ends): WindowTables<'_> = by_pressure(
            pp,
            (&o3::INI1[..], &o3::IFIN1[..]),
            (&o3::INI2[..], &o3::IFIN2[..]),
            (&o3::INI3[..], &o3::IFIN3[..]),
        );
        let bin = vp.min(ends.len() - 1);
        if let Some(range) = window_range(i64::from(starts[bin]), i64::from(ends[bin])) {
            let lines = AirLines {
                frequency: &o3::FRE,
                intensity: &o3::FLIN,
                energy: &o3::EL,
                broadening: &o3::BRDO3AIR,
                exponent: Exponent::PerLine(&o3::TEXPO3),
            };
            sum = lines.sum(nu, tt, pp, 48.0, range);
        }
    }
    main_scaled(sum, nu, tt, 0.649698902072 * tt * tt.sqrt())
}

/// (16)O3 in the ν1 state (source 18), below 999.99 GHz.
pub(super) fn o3_nu1(tt: f64, pp: f64, nu: f64, vp: usize) -> Complex64 {
    let mut sum = ZERO;
    if nu < 999.99
        && let Some(range) = window(vp, 2, (&o3_v1::INI1, &o3_v1::IFIN1))
    {
        let lines = AirLines {
            frequency: &o3_v1::FRE,
            intensity: &o3_v1::FLIN,
            energy: &o3_v1::EL,
            broadening: &o3_v1::BRDO3AIR,
            exponent: Exponent::PerLine(&o3_v1::TEXPO3),
        };
        sum = lines.sum(nu, tt, pp, 48.0, range);
    }
    main_scaled(sum, nu, tt, 0.6531261 * tt * tt.sqrt())
}

/// (16)O3 in the ν2 state (source 17), up to 999.9 GHz. ATM's window for
/// this state spans four bins either side.
pub(super) fn o3_nu2(tt: f64, pp: f64, nu: f64, vp: usize) -> Complex64 {
    if nu > 999.9 {
        return ZERO;
    }
    let Some(range) = window(vp, 4, (&o3_v2::INI1, &o3_v2::IFIN1)) else {
        return ZERO;
    };
    let lines = AirLines {
        frequency: &o3_v2::FRE,
        intensity: &o3_v2::FLIN,
        energy: &o3_v2::EL,
        broadening: &o3_v2::BRDO3AIR,
        exponent: Exponent::PerLine(&o3_v2::TEXPO3),
    };
    main_scaled(
        lines.sum(nu, tt, pp, 48.0, range),
        nu,
        tt,
        0.647739935 * tt * tt.sqrt(),
    )
}

/// (16)O3 in the ν3 state (source 19), up to 999.9 GHz.
pub(super) fn o3_nu3(tt: f64, pp: f64, nu: f64, vp: usize) -> Complex64 {
    if nu > 999.9 {
        return ZERO;
    }
    let Some(range) = window(vp, 2, (&o3_v3::INI1, &o3_v3::IFIN1)) else {
        return ZERO;
    };
    let lines = AirLines {
        frequency: &o3_v3::FRE,
        intensity: &o3_v3::FLIN,
        energy: &o3_v3::EL,
        broadening: &o3_v3::BRDO3AIR,
        exponent: Exponent::PerLine(&o3_v3::TEXPO3),
    };
    main_scaled(
        lines.sum(nu, tt, pp, 48.0, range),
        nu,
        tt,
        1.321477358 * tt * tt.sqrt(),
    )
}

/// One isotopologue's catalogue and constants.
struct Isotopologue<'a> {
    lines: AirLines<'a>,
    tables: [WindowTables<'a>; 3],
    mmol: f64,
    q_per_t15: f64,
}

impl Isotopologue<'_> {
    /// ATM's isotopologue sum: up to 999.9 GHz, with pressure-dependent
    /// windows two bins either side.
    fn refractivity(&self, tt: f64, pp: f64, nu: f64, vp: usize) -> Complex64 {
        if nu > 999.9 {
            return ZERO;
        }
        let [high, mid, low] = self.tables;
        let Some(range) = window(vp, 2, by_pressure(pp, high, mid, low)) else {
            return ZERO;
        };
        let q = self.q_per_t15 * tt * tt.sqrt();
        isotope_scaled(self.lines.sum(nu, tt, pp, self.mmol, range), nu, tt, q)
    }
}

/// (16)O(16)O(18)O (source 20).
pub(super) fn o3_668(tt: f64, pp: f64, nu: f64, vp: usize) -> Complex64 {
    use o3_16o16o18o as t;
    Isotopologue {
        lines: AirLines {
            frequency: &t::FRE,
            intensity: &t::FLIN,
            energy: &t::EL,
            broadening: &t::BRDO3AIR,
            exponent: Exponent::PerLine(&t::TEXPO3),
        },
        tables: [
            (&t::INI1, &t::IFIN1),
            (&t::INI2, &t::IFIN2),
            (&t::INI3, &t::IFIN3),
        ],
        mmol: 50.0,
        q_per_t15: 1.387429043,
    }
    .refractivity(tt, pp, nu, vp)
}

/// (16)O(16)O(17)O (source 21).
pub(super) fn o3_667(tt: f64, pp: f64, nu: f64, vp: usize) -> Complex64 {
    use o3_16o16o17o as t;
    Isotopologue {
        lines: AirLines {
            frequency: &t::FRE,
            intensity: &t::FLIN,
            energy: &t::EL,
            broadening: &t::BRDO3AIR,
            exponent: Exponent::PerLine(&t::TEXPO3),
        },
        tables: [
            (&t::INI1, &t::IFIN1),
            (&t::INI2, &t::IFIN2),
            (&t::INI3, &t::IFIN3),
        ],
        mmol: 49.0,
        q_per_t15: 1.344455353,
    }
    .refractivity(tt, pp, nu, vp)
}

/// (16)O(18)O(16)O (source 22).
pub(super) fn o3_686(tt: f64, pp: f64, nu: f64, vp: usize) -> Complex64 {
    use o3_16o18o16o as t;
    Isotopologue {
        lines: AirLines {
            frequency: &t::FRE,
            intensity: &t::FLIN,
            energy: &t::EL,
            broadening: &t::BRDO3AIR,
            exponent: Exponent::PerLine(&t::TEXPO3),
        },
        tables: [
            (&t::INI1, &t::IFIN1),
            (&t::INI2, &t::IFIN2),
            (&t::INI3, &t::IFIN3),
        ],
        mmol: 50.0,
        q_per_t15: 0.678068387,
    }
    .refractivity(tt, pp, nu, vp)
}

/// (16)O(17)O(16)O (source 23).
pub(super) fn o3_676(tt: f64, pp: f64, nu: f64, vp: usize) -> Complex64 {
    use o3_16o17o16o as t;
    Isotopologue {
        lines: AirLines {
            frequency: &t::FRE,
            intensity: &t::FLIN,
            energy: &t::EL,
            broadening: &t::BRDO3AIR,
            exponent: Exponent::PerLine(&t::TEXPO3),
        },
        tables: [
            (&t::INI1, &t::IFIN1),
            (&t::INI2, &t::IFIN2),
            (&t::INI3, &t::IFIN3),
        ],
        mmol: 49.0,
        q_per_t15: 0.664313224,
    }
    .refractivity(tt, pp, nu, vp)
}
