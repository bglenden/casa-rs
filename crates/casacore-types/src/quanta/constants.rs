// SPDX-License-Identifier: LGPL-3.0-or-later
//! Physical constants as [`Quantity`] values.
//!
//! This module provides the ~20 fundamental constants from C++ `casa::QC`,
//! each as a lazily-initialised `&'static Quantity`.
//!
//! # Examples
//!
//! ```
//! use casacore_types::quanta::constants;
//!
//! let c = constants::c();
//! assert!((c.value() - 2.99792458e8).abs() < 1.0);
//! assert_eq!(c.unit().name(), "m/s");
//! ```

use std::sync::OnceLock;

use crate::quanta::quantity::Quantity;

macro_rules! define_constant {
    ($name:ident, $value:expr, $unit:expr, $doc:expr) => {
        #[doc = $doc]
        pub fn $name() -> &'static Quantity {
            static VAL: OnceLock<Quantity> = OnceLock::new();
            VAL.get_or_init(|| {
                Quantity::new($value, $unit).expect(concat!("QC::", stringify!($name)))
            })
        }
    };
}

define_constant!(
    c,
    2.997_924_58e8,
    "m/s",
    "Speed of light in vacuum (m/s). Exact SI definition."
);

define_constant!(
    gravitational,
    6.672_59e-11,
    "N.m2/kg2",
    "Newtonian gravitational constant (N m² kg⁻²)."
);

define_constant!(h, 6.626_075_5e-34, "J.s", "Planck constant (J s).");

define_constant!(
    hi_line,
    1_420.405_751_786e6,
    "Hz",
    "HI 21-cm line rest frequency (Hz)."
);

define_constant!(
    gas_constant,
    8.314_510,
    "J/K/mol",
    "Molar gas constant R (J K⁻¹ mol⁻¹)."
);

define_constant!(
    avogadro,
    6.022_136_7e23,
    "/mol",
    "Avogadro constant Nₐ (mol⁻¹)."
);

define_constant!(
    electron_charge,
    1.602_177_33e-19,
    "C",
    "Elementary charge e (C)."
);

define_constant!(proton_mass, 1.672_623_1e-27, "kg", "Proton mass mₚ (kg).");

define_constant!(
    proton_electron_ratio,
    1_836.152_701,
    "",
    "Proton-to-electron mass ratio mₚ/mₑ (dimensionless)."
);

define_constant!(
    vacuum_permeability,
    1.256_637_061_435_917_3e-6,
    "H/m",
    "Permeability of vacuum μ₀ = 4π × 10⁻⁷ (H/m)."
);

define_constant!(
    vacuum_permittivity,
    8.854_187_817_620_39e-12,
    "F/m",
    "Permittivity of vacuum ε₀ = 1/(μ₀ c²) (F/m)."
);

define_constant!(
    boltzmann,
    1.380_657_4e-23,
    "J/K",
    "Boltzmann constant k = R/Nₐ (J/K)."
);

define_constant!(
    faraday,
    96_485.309,
    "C/mol",
    "Faraday constant F = Nₐ e (C/mol)."
);

define_constant!(
    electron_mass,
    9.109_389_7e-31,
    "kg",
    "Electron mass mₑ = mₚ / (mₚ/mₑ) (kg)."
);

define_constant!(
    electron_radius,
    2.8179e-15,
    "m",
    "Classical electron radius rₑ (m)."
);

define_constant!(bohr_radius, 5.2918e-11, "m", "Bohr radius a₀ (m).");

define_constant!(solar_radius, 6.9599e8, "m", "Solar radius R₀ (m).");

define_constant!(
    iau_k2,
    2.959_122_082_855_911e-4,
    "AU3/d2/S0",
    "IAU Gaussian gravitational constant squared k² (AU³ d⁻² S₀⁻¹)."
);

define_constant!(quarter_turn, 90.0, "deg", "Quarter turn (90°).");

define_constant!(half_turn, 180.0, "deg", "Half turn (180°).");

define_constant!(full_turn, 360.0, "deg", "Full turn (360°).");

#[cfg(test)]
mod tests {
    use super::*;

    fn close(a: f64, b: f64, rel_tol: f64) -> bool {
        let diff = (a - b).abs();
        let scale = a.abs().max(b.abs()).max(1e-30);
        diff / scale < rel_tol
    }

    #[test]
    fn speed_of_light() {
        let val = c();
        assert!(close(val.value(), 2.997_924_58e8, 1e-12));
        assert_eq!(val.unit().name(), "m/s");
    }

    #[test]
    fn planck() {
        let val = h();
        assert!(close(val.value(), 6.626_075_5e-34, 1e-12));
    }

    #[test]
    fn boltzmann_from_r_na() {
        let k_val = boltzmann();
        // k = R / NA
        let expected = 8.314_510 / 6.022_136_7e23;
        assert!(close(k_val.value(), expected, 1e-6));
    }

    #[test]
    fn quarter_turn_degrees() {
        let q = quarter_turn();
        assert_eq!(q.value(), 90.0);
        assert_eq!(q.unit().name(), "deg");
    }

    #[test]
    fn full_turn_in_rad() {
        let ft = full_turn();
        let rad_unit = crate::quanta::unit::Unit::new("rad").unwrap();
        let in_rad = ft.get_value_in(&rad_unit).unwrap();
        assert!(close(in_rad, 2.0 * std::f64::consts::PI, 1e-12));
    }
}
