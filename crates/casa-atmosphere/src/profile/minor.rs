// SPDX-License-Identifier: LGPL-3.0-or-later
// Ported from ATM's ATMProfile.cc (`st76`, `poli2`), (c) Instituto de
// Estructura de la Materia, Pardo et al., LGPL-2.1-or-later.
//! Minor-gas number densities from the US Standard 1976 family of profiles.

use super::tables::{
    ST76_ALT as ALT, ST76_CO as CO, ST76_DEN as DEN, ST76_HCL as HCL, ST76_HCN as HCN,
    ST76_N2O as N2O, ST76_NO2 as NO2, ST76_OZONE as OZONE, ST76_SO2 as SO2,
};

const AVOGADRO: f64 = 6.022045e23;
const AIR_MOLAR_MASS: f64 = 28.964;

/// Minor-gas number densities in molecules/cm³.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(super) struct MinorGases {
    pub(super) o3: f64,
    pub(super) n2o: f64,
    pub(super) co: f64,
    pub(super) no2: f64,
    pub(super) so2: f64,
    pub(super) hcl: f64,
    pub(super) hcn: f64,
}

/// The minor gases at `height_km` above sea level for reference profile
/// `reference` (1 to 6) and seasonal/diurnal ozone model `ozone_model`
/// (1 to 4): ATM's `st76`.
///
/// Each profile is interpolated by the parabola through the three tabulated
/// levels around the height. The total air density converts the mixing
/// ratios (ppmv) to number densities. ATM then scales ozone by layer for its
/// Chajnantor models, N2O above 30 km, and HCl and HCN above fixed levels
/// (ATM's 2024 fits to Chajnantor spectra).
pub(super) fn minor_gases(height_km: f64, reference: usize, ozone_model: u32) -> MinorGases {
    if !(0.0..=120.0).contains(&height_km) {
        return MinorGases::default();
    }
    // The first tabulated level above the height, and its neighbours.
    let Some(above) = ALT.iter().position(|&level| height_km < level) else {
        return MinorGases::default();
    };
    let (i1, i2, i3) = match above {
        0 => (0, 1, 2),
        49 => (47, 48, 49),
        k => (k - 1, k, k + 1),
    };
    let (x1, x2, x3) = (ALT[i1], ALT[i2], ALT[i3]);
    if x1 == 0.0 && x2 == 0.0 && x3 == 0.0 {
        return MinorGases::default();
    }
    let at = |table: &[f64; 50]| parabola(height_km, x1, x2, x3, table[i1], table[i2], table[i3]);
    let r = reference - 1;
    // Air density in g/m³.
    let density = at(&DEN[r]) * AIR_MOLAR_MASS * 1e6 / AVOGADRO;
    let per_cm3 = |mixing_ratio: f64| mixing_ratio * 1e-12 * density * AVOGADRO / AIR_MOLAR_MASS;

    let mut o3 = per_cm3(at(&OZONE[r]));
    o3 *= ozone_factor(ozone_model, i2);
    let mut n2o = per_cm3(at(&N2O[r]));
    if height_km > 36.0 {
        n2o *= 1.5;
    } else if height_km > 30.0 {
        n2o *= 2.5;
    }
    let no2 = per_cm3(at(&NO2));
    let mut hcl = per_cm3(at(&HCL));
    if i2 >= 30 {
        hcl *= 3.5;
    }
    let mut hcn = per_cm3(at(&HCN));
    hcn *= if i2 < 25 { 0.9 } else { 2.5 };
    let so2 = per_cm3(at(&SO2));
    let co = per_cm3(at(&CO[r]));
    MinorGases {
        o3,
        n2o,
        co,
        no2,
        so2,
        hcl,
        hcn,
    }
}

/// ATM's ozone scaling for its seasonal/diurnal models, by the middle of the
/// three interpolation levels.
fn ozone_factor(ozone_model: u32, i2: usize) -> f64 {
    // (first upper level, factor below level 24, factor up to the split,
    // factor below level 32, below level 35, above)
    let (split, low, low_upper, mid, high, top) = match ozone_model {
        1 => (29, 0.650, 0.860, 1.35, 1.60, 2.45),
        2 => (29, 0.650, 0.890, 1.45, 1.6, 2.55),
        3 => (30, 0.500, 0.800, 1.65, 1.70, 2.40),
        4 => (30, 0.500, 0.800, 1.65, 1.70, 2.50),
        _ => return 1.0,
    };
    if i2 < split {
        if i2 < 24 { low } else { low_upper }
    } else if i2 < 32 {
        mid
    } else if i2 < 35 {
        high
    } else {
        top
    }
}

/// The parabola through (x1, y1), (x2, y2), (x3, y3), evaluated at `x`:
/// ATM's `poli2`.
fn parabola(x: f64, x1: f64, x2: f64, x3: f64, y1: f64, y2: f64, y3: f64) -> f64 {
    let mut c = (y3 - y2) * (x2 - x1) - (y2 - y1) * (x3 - x2);
    let mut b = (x2 - x1) * (x3 * x3 - x2 * x2) - (x2 * x2 - x1 * x1) * (x3 - x2);
    c /= b;
    b = (y2 - y1) - c * (x2 * x2 - x1 * x1);
    b /= x2 - x1;
    let a = y1 - c * x1 * x1 - b * x1;
    a + b * x + c * x.powf(2.0)
}
