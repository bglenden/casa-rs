// SPDX-License-Identifier: LGPL-3.0-or-later
// Ported from ATM's ATMRefractiveIndex.cc, (c) Instituto de Estructura de la
// Materia, Pardo et al., LGPL-2.1-or-later.
//! Complex refractivity of the atmospheric gases (ATM's `RefractiveIndex`).
//!
//! For each absorbing species ATM sums Van Vleck–Weisskopf line profiles
//! over its line catalogue,
//!
//! ```text
//! F(ν) = (ν/νₗ) [ (1 − iδ)/(νₗ − ν − iΔν) − (1 + iδ)/(νₗ + ν + iΔν) ]
//! ```
//!
//! with pressure (and, where it matters, Doppler) broadening Δν and, for
//! O2, line interference δ. Each line is weighted by its intensity and
//! Boltzmann factor, and the sum by the dipole moment and partition
//! function. The result is the species' *specific refractivity*: a complex
//! number whose imaginary part is the absorption cross-section (m²) and
//! whose real part is the phase delay cross-section (rad·m²). Multiplying by
//! a number density gives the absorption (m⁻¹) and phase (rad/m) per unit
//! path; ATM's layer refractivities are those products
//! ([`o2_refractivity`], [`h2o_refractivity`], …).
//!
//! The water and dry continua (collision-induced absorption) are empirical
//! and already per unit path.
//!
//! Frequencies are in GHz, temperatures in kelvin and pressures in hPa, as
//! in ATM. ATM restricts each line sum to the lines within a few GHz of the
//! frequency, through window tables indexed by 2-GHz frequency bins; the
//! port keeps those windows, so it sums exactly the lines ATM sums.

mod minor;
mod oxygen;
mod ozone;
mod tables;
#[cfg(test)]
mod tests;
mod water;

use num_complex::Complex64;

/// An ATM opacity source, numbered as ATM numbers them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum Species {
    /// (16)O(16)O lines from the ground vibrational state.
    O2 = 1,
    /// (16)O(16)O lines from the first vibrational state.
    O2Vibrational = 2,
    /// (16)O(18)O lines.
    O2Isotope18 = 3,
    /// (16)O(17)O lines. ATM returns zero for this species.
    O2Isotope17 = 4,
    /// CO lines.
    Co = 5,
    /// N2O lines.
    N2o = 6,
    /// NO2 lines.
    No2 = 7,
    /// SO2 lines.
    So2 = 8,
    /// The water-vapour continuum.
    WaterContinuum = 9,
    /// The dry-air continuum.
    DryContinuum = 10,
    /// H2(16)O lines from the ground vibrational state.
    H2o = 11,
    /// H2(16)O lines from the first bending state.
    H2oBending = 12,
    /// H2(18)O lines.
    H2oIsotope18 = 13,
    /// H2(17)O lines.
    H2oIsotope17 = 14,
    /// HDO lines.
    Hdo = 15,
    /// (16)O3 lines from the ground vibrational state.
    O3 = 16,
    /// (16)O3 lines from the ν2 state.
    O3Nu2 = 17,
    /// (16)O3 lines from the ν1 state.
    O3Nu1 = 18,
    /// (16)O3 lines from the ν3 state.
    O3Nu3 = 19,
    /// (16)O(16)O(18)O lines.
    O3Isotope668 = 20,
    /// (16)O(16)O(17)O lines.
    O3Isotope667 = 21,
    /// (16)O(18)O(16)O lines.
    O3Isotope686 = 22,
    /// (16)O(17)O(16)O lines.
    O3Isotope676 = 23,
    /// HCl lines.
    Hcl = 24,
    /// HCN lines.
    Hcn = 25,
}

impl Species {
    /// Every species, in ATM's numbering.
    pub const ALL: [Species; 25] = [
        Self::O2,
        Self::O2Vibrational,
        Self::O2Isotope18,
        Self::O2Isotope17,
        Self::Co,
        Self::N2o,
        Self::No2,
        Self::So2,
        Self::WaterContinuum,
        Self::DryContinuum,
        Self::H2o,
        Self::H2oBending,
        Self::H2oIsotope18,
        Self::H2oIsotope17,
        Self::Hdo,
        Self::O3,
        Self::O3Nu2,
        Self::O3Nu1,
        Self::O3Nu3,
        Self::O3Isotope668,
        Self::O3Isotope667,
        Self::O3Isotope686,
        Self::O3Isotope676,
        Self::Hcl,
        Self::Hcn,
    ];
}

/// The specific refractivity of `species` (ATM's
/// `mkEarthSpecificRefractivity`): the line species' cross-sections in
/// (rad·m², m²) per molecule of the parent gas, or the continua's
/// refractivity per unit path in (rad/m, 1/m)… as ATM defines them.
///
/// The isotopologue and vibrational-state species include their fraction
/// of the parent gas (O2, H2O or O3), so the parent's number density
/// multiplies them all. `water_vapour_pressure_hpa` enters the water and
/// O2 broadening and the continua.
#[must_use]
pub fn specific_refractivity(
    species: Species,
    temperature_k: f64,
    pressure_hpa: f64,
    water_vapour_pressure_hpa: f64,
    frequency_ghz: f64,
) -> Complex64 {
    let (tt, pp, eh2o, nu) = (
        temperature_k,
        pressure_hpa,
        water_vapour_pressure_hpa,
        frequency_ghz,
    );
    let ratios = IsotopicRatios::at(tt);
    let vp = frequency_bin(nu);
    match species {
        Species::Co => minor::co(tt, pp, nu),
        Species::N2o => minor::n2o(tt, pp, nu),
        Species::No2 => minor::no2(tt, pp, nu),
        Species::So2 => minor::so2(tt, pp, nu),
        Species::Hcl => minor::hcl(tt, pp, nu),
        Species::Hcn => minor::hcn(tt, pp, nu),
        Species::WaterContinuum => water::continuum(tt, pp, eh2o, nu),
        Species::DryContinuum => water::dry_continuum(tt, pp, eh2o, nu),
        Species::O2 => oxygen::o2(tt, pp, eh2o, nu) * ratios.o2,
        Species::O2Vibrational => oxygen::o2_vibrational(tt, pp, eh2o, nu) * ratios.o2_vibrational,
        Species::O2Isotope18 => oxygen::o2_isotope18(tt, pp, eh2o, nu) * ratios.o2_isotope18,
        Species::O2Isotope17 => oxygen::o2_isotope17(nu) * ratios.o2_isotope17,
        Species::H2o => water::h2o(tt, pp, eh2o, nu) * ratios.h2o,
        Species::H2oBending => water::h2o_bending(tt, pp, eh2o, nu) * ratios.h2o_bending,
        Species::H2oIsotope18 => water::h2o_isotope18(tt, pp, eh2o, nu) * ratios.h2o_isotope18,
        Species::H2oIsotope17 => water::h2o_isotope17(tt, pp, eh2o, nu) * ratios.h2o_isotope17,
        Species::Hdo => water::hdo(tt, pp, nu) * ratios.hdo,
        Species::O3 => ozone::o3(tt, pp, nu, vp) * ratios.o3,
        Species::O3Nu2 => ozone::o3_nu2(tt, pp, nu, vp) * ratios.o3_nu2,
        Species::O3Nu1 => ozone::o3_nu1(tt, pp, nu, vp) * ratios.o3_nu1,
        Species::O3Nu3 => ozone::o3_nu3(tt, pp, nu, vp) * ratios.o3_nu3,
        Species::O3Isotope668 => ozone::o3_668(tt, pp, nu, vp) * ratios.o3_668,
        Species::O3Isotope667 => ozone::o3_667(tt, pp, nu, vp) * ratios.o3_667,
        Species::O3Isotope686 => ozone::o3_686(tt, pp, nu, vp) * ratios.o3_686,
        Species::O3Isotope676 => ozone::o3_676(tt, pp, nu, vp) * ratios.o3_676,
    }
}

/// Boltzmann's constant as ATM writes it, J/K.
const BOLTZMANN: f64 = 1.380662e-23;
/// ATM's O2 mixing ratio.
const O2_MIXING_RATIO: f64 = 0.2092;

/// The O2 lines' refractivity per unit path, (rad/m, 1/m): the specific
/// refractivity times the O2 number density at `pressure_hpa`
/// (ATM's `getRefractivity_16o16o` and its isotopologue siblings).
#[must_use]
pub fn o2_refractivity(
    species: Species,
    temperature_k: f64,
    pressure_hpa: f64,
    water_vapour_pressure_hpa: f64,
    frequency_ghz: f64,
) -> Complex64 {
    o2_per_path(
        specific_refractivity(
            species,
            temperature_k,
            pressure_hpa,
            water_vapour_pressure_hpa,
            frequency_ghz,
        ),
        temperature_k,
        pressure_hpa,
    )
}

/// An O2 specific refractivity (or a sum of them) times the O2 number
/// density, in ATM's order of operations.
pub(crate) fn o2_per_path(specific: Complex64, temperature_k: f64, pressure_hpa: f64) -> Complex64 {
    specific * O2_MIXING_RATIO * pressure_hpa * 100.0 / (BOLTZMANN * temperature_k)
}

/// A water specific refractivity (or a sum of them) times the water number
/// density for partial pressure `water_vapour_pressure_hpa`, as ATM writes
/// it.
pub(crate) fn h2o_per_path(
    specific: Complex64,
    temperature_k: f64,
    water_vapour_pressure_hpa: f64,
) -> Complex64 {
    specific * (6.023e23 * water_vapour_pressure_hpa * 217.0 / (temperature_k * 18.005059688))
}

/// A water line species' refractivity per unit path, (rad/m, 1/m): the
/// specific refractivity times the water number density for the partial
/// pressure `water_vapour_pressure_hpa` (ATM's `getRefractivity_hh16o` and
/// its siblings).
#[must_use]
pub fn h2o_refractivity(
    species: Species,
    temperature_k: f64,
    pressure_hpa: f64,
    water_vapour_pressure_hpa: f64,
    frequency_ghz: f64,
) -> Complex64 {
    h2o_per_path(
        specific_refractivity(
            species,
            temperature_k,
            pressure_hpa,
            water_vapour_pressure_hpa,
            frequency_ghz,
        ),
        temperature_k,
        water_vapour_pressure_hpa,
    )
}

/// A species' refractivity per unit path for a number density in
/// molecules/m³, (rad/m, 1/m): ATM's `getRefractivity_16o16o16o` and its
/// siblings for the ozone species, and its `getEarthSpecificRefractivity_*`
/// times the density for the minor gases. These species take no water
/// vapour pressure.
#[must_use]
pub fn density_refractivity(
    species: Species,
    temperature_k: f64,
    pressure_hpa: f64,
    frequency_ghz: f64,
    number_density_per_m3: f64,
) -> Complex64 {
    specific_refractivity(species, temperature_k, pressure_hpa, 0.0, frequency_ghz)
        * number_density_per_m3
}

/// The fractions of the parent gas in each isotopologue and vibrational
/// state at temperature `tt`, as ATM computes them.
///
/// Two of ATM's fractions are corrected (`docs/CASA (C++) bugs.md`): its
/// H2(17)O fraction uses the (18)O abundance, and its HDO fraction's
/// denominator reads `2-0*abun_d` (that is, 2) where `2.0*abun_d` was meant.
/// Here H2(17)O takes the (17)O abundance, and HDO the water parent's
/// denominator, as H2(18)O does.
struct IsotopicRatios {
    o2: f64,
    o2_vibrational: f64,
    o2_isotope18: f64,
    o2_isotope17: f64,
    h2o: f64,
    h2o_bending: f64,
    h2o_isotope18: f64,
    h2o_isotope17: f64,
    hdo: f64,
    o3: f64,
    o3_nu1: f64,
    o3_nu2: f64,
    o3_nu3: f64,
    o3_668: f64,
    o3_667: f64,
    o3_686: f64,
    o3_676: f64,
}

impl IsotopicRatios {
    fn at(tt: f64) -> Self {
        const ABUN_18O: f64 = 0.0020439;
        const ABUN_17O: f64 = 0.0003750;
        const ABUN_D: f64 = 0.000298444;
        const TEX_H2OVIB: f64 = 2322.92;
        const TEX_O2VIB: f64 = 2239.273;
        const TEX_O3NU2: f64 = 1009.50;
        const TEX_O3NU1: f64 = 1588.41;
        const TEX_O3NU3: f64 = 1500.48;
        let o3_parent = 1.0 + 3.0 * (ABUN_18O + ABUN_17O);
        let o2_parent = 1.0 + 2.0 * (ABUN_18O + ABUN_17O);
        let h2o_parent = 1.0 + ABUN_18O + ABUN_17O + 2.0 * ABUN_D;
        Self {
            o3: (1.0 - (-TEX_O3NU1 / tt).exp() - (-TEX_O3NU2 / tt).exp() - (-TEX_O3NU3 / tt).exp())
                / o3_parent,
            o3_nu1: (-TEX_O3NU1 / tt).exp() / o3_parent,
            o3_nu2: (-TEX_O3NU2 / tt).exp() / o3_parent,
            o3_nu3: (-TEX_O3NU3 / tt).exp() / o3_parent,
            o3_668: 2.0 * ABUN_18O / o3_parent,
            o3_667: 2.0 * ABUN_17O / o3_parent,
            o3_686: ABUN_18O / o3_parent,
            o3_676: ABUN_17O / o3_parent,
            o2: (1.0 - (-TEX_O2VIB / tt).exp()) / o2_parent,
            o2_vibrational: (-TEX_O2VIB / tt).exp() / o2_parent,
            o2_isotope18: 2.0 * ABUN_18O / o2_parent,
            o2_isotope17: 2.0 * ABUN_17O / o2_parent,
            h2o: (1.0 - (-TEX_H2OVIB / tt).exp()) / (1.0 + ABUN_18O + ABUN_17O + 2.0 * ABUN_D),
            h2o_bending: (-TEX_H2OVIB / tt).exp() / (1.0 + ABUN_18O + ABUN_17O + 2.0 * ABUN_D),
            h2o_isotope18: ABUN_18O / h2o_parent,
            h2o_isotope17: ABUN_17O / h2o_parent,
            hdo: 2.0 * ABUN_D / h2o_parent,
        }
    }
}

// Constants ATM's line sums share.
/// π as ATM writes it, to ten digits; the port keeps it so its numbers are
/// ATM's.
#[expect(
    clippy::approx_constant,
    reason = "ATM's own truncated π, kept for parity"
)]
const PI: f64 = 3.141592654;
/// (8π³/3hc)·(1e-18)², which turns Debye² into the line-strength unit.
const PICUBE8DIV3HCESU: f64 = 4.1623755e-19;
/// h/k in K/GHz.
const H_OVER_K: f64 = 0.047992745509;

/// ATM's frequency bin: the index of the 2-GHz bin `frequency_ghz` falls
/// in, which selects the window of catalogue lines to sum (`vpIndex`).
#[must_use]
pub(crate) fn frequency_bin(frequency_ghz: f64) -> usize {
    if frequency_ghz < 1.0 {
        0
    } else {
        // C's round() rounds halves away from zero, as f64::round does.
        let bin = ((frequency_ghz + 1.0) / 2.0).round() as i64;
        usize::try_from(bin - 1).unwrap_or(0)
    }
}

/// A pair of ATM window tables: 1-based first and last catalogue line for
/// each frequency bin.
type WindowTables<'a> = (&'a [u16], &'a [u16]);

/// The catalogue lines (inclusive range) ATM sums for frequency bin `vp`,
/// `None` when ATM sums none. See [`window_bounds`].
fn window(
    vp: usize,
    half_width: usize,
    (starts, ends): WindowTables<'_>,
) -> Option<std::ops::RangeInclusive<usize>> {
    let (start, end) = window_bounds(vp, half_width, (starts, ends), 0);
    window_range(start, end)
}

/// ATM's window bounds `ini` and `ifin` for frequency bin `vp`, as signed
/// values: the start is looked up at bin `vp − half_width` and the end at
/// `vp + half_width` (clamped to the table), both less one, or the start is
/// `first` when `vp ≤ half_width`.
fn window_bounds(
    vp: usize,
    half_width: usize,
    (starts, ends): WindowTables<'_>,
    first: i64,
) -> (i64, i64) {
    let last = ends.len() - 1;
    let end = i64::from(ends[(vp + half_width).min(last)]) - 1;
    let start = if vp > half_width {
        i64::from(starts[(vp - half_width).min(last)]) - 1
    } else {
        first
    };
    (start, end)
}

/// The inclusive line range for ATM's `ini` and `ifin` taken as signed.
///
/// ATM's indices are unsigned and it skips the sum when `ifin == 0 || ifin
/// < ini`. An empty table entry (0) less one wraps to the largest value: a
/// wrapped start is then above any end, and a wrapped end makes the loop
/// bound `ifin + 1` wrap to 0. So the sum is empty when the end is 0 or
/// negative, the start negative, or the end before the start.
pub(crate) fn window_range(start: i64, end: i64) -> Option<std::ops::RangeInclusive<usize>> {
    if end <= 0 || start < 0 || end < start {
        return None;
    }
    Some(usize::try_from(start).ok()?..=usize::try_from(end).ok()?)
}

/// ATM's pressure-dependent choice among its three window tables: the
/// narrow windows below 25 hPa, the middle ones below 300 hPa, the wide
/// ones above.
fn by_pressure<T>(pp: f64, high: T, mid: T, low: T) -> T {
    if pp < 25.0 {
        low
    } else if pp < 300.0 {
        mid
    } else {
        high
    }
}

const ZERO: Complex64 = Complex64::new(0.0, 0.0);

/// An air-broadened line catalogue: line frequencies (GHz), intensities,
/// lower-state energies (K), air-broadening coefficients (MHz/hPa) and their
/// temperature exponents.
struct AirLines<'a> {
    frequency: &'a [f64],
    intensity: &'a [f64],
    energy: &'a [f64],
    broadening: &'a [f64],
    exponent: Exponent<'a>,
}

/// A catalogue's broadening temperature exponents.
#[derive(Clone, Copy)]
enum Exponent<'a> {
    /// One exponent for every line.
    Fixed(f64),
    /// One exponent per line.
    PerLine(&'a [f64]),
}

impl AirLines<'_> {
    /// The sum over `lines` of the profiles weighted by intensity ·
    /// exp(−El/T) · νₗ, for molecular mass `mmol`, as ATM's minor-gas and
    /// ozone functions accumulate it.
    fn sum(
        &self,
        nu: f64,
        tt: f64,
        pp: f64,
        mmol: f64,
        lines: std::ops::RangeInclusive<usize>,
    ) -> Complex64 {
        let mut sum = ZERO;
        for i in lines {
            let exponent = match self.exponent {
                Exponent::Fixed(exponent) => exponent,
                Exponent::PerLine(exponents) => exponents[i],
            };
            let width = line_broadening(
                self.frequency[i],
                tt,
                pp,
                mmol,
                self.broadening[i] * 0.001,
                exponent,
            );
            let shape = line_shape(nu, self.frequency[i], width, 0.0);
            sum += shape * self.intensity[i] * (-self.energy[i] / tt).exp() * self.frequency[i];
        }
        sum
    }
}

/// The Van Vleck–Weisskopf profile of a line at `vl` GHz with width `dv`
/// and interference `itf`, at `v` GHz (ATM's `lineshape`).
fn line_shape(v: f64, vl: f64, dv: f64, itf: f64) -> Complex64 {
    Complex64::new(v / vl, 0.0)
        * (Complex64::new(1.0, -itf) / Complex64::new(vl - v, -dv)
            - Complex64::new(1.0, itf) / Complex64::new(vl + v, dv))
}

/// A line's width in GHz: pressure broadening `dv0_lines` (GHz/hPa) with
/// temperature exponent `texp_lines`, combined with the Doppler width when
/// that is comparable (Janssen, *Atmospheric Remote Sensing*, p. 59).
fn line_broadening(nu: f64, temp: f64, pr: f64, mmol: f64, dv0_lines: f64, texp_lines: f64) -> f64 {
    let dv0 = dv0_lines * pr * (300.0 / temp).powf(texp_lines);
    doppler_combined(dv0, 4.30e-7 * nu * (temp / mmol).sqrt())
}

/// An O2 line's width in GHz, with self (dry) and water broadening.
fn line_broadening_o2(nu: f64, temp: f64, pr: f64, eh2o: f64, mmol: f64, b1: f64, b2: f64) -> f64 {
    let dv0 = 1e-3 * b1 * ((pr - eh2o) * (300.0 / temp).powf(b2) + 1.1 * eh2o * 300.0 / temp);
    doppler_combined(dv0, 4.30e-7 * nu * (temp / mmol).sqrt())
}

/// An O2 line's interference coefficient in GHz.
fn interference_o2(temp: f64, pp: f64, b3: f64, b4: f64) -> f64 {
    1e-3 * (b3 + b4 * (300.0 / temp)) * pp * (300.0 / temp).powf(0.8)
}

/// A water line's width in GHz, with air and self broadening. A
/// non-positive self coefficient selects ATM's default coefficients.
#[expect(
    clippy::too_many_arguments,
    reason = "ATM's broadening takes four tabulated coefficients"
)]
fn line_broadening_water(
    nu: f64,
    temp: f64,
    pr: f64,
    eh2o: f64,
    b1: f64,
    b2: f64,
    b3: f64,
    b4: f64,
) -> f64 {
    let dv0 = if b2 > 0.0 {
        1e-3 * b1 * ((pr - eh2o) * (300.0 / temp).powf(b3) + b2 * eh2o * (300.0 / temp).powf(b4))
    } else {
        1e-3 * b1
            * ((pr - eh2o) * (300.0 / temp).powf(0.68) + 4.50 * eh2o * (300.0 / temp).powf(0.80))
    };
    doppler_combined(dv0, 3.58e-7 * nu * (temp / 18.0).sqrt())
}

/// An H2(18)O or H2(17)O line's width in GHz.
fn line_broadening_rare_water(
    temp: f64,
    pr: f64,
    eh2o: f64,
    dv0: f64,
    dvlm: f64,
    temp_exp: f64,
) -> f64 {
    // Avogadro's number times Boltzmann's constant.
    let rho = 18.0 * eh2o * 100.0 / (8.315727226 * temp);
    let c2 = 4.6e-03 * rho * temp / pr;
    (dv0 * (pr / 1013.0) / (temp / 300.0).powf(temp_exp)) * (1.0 + c2 * (dvlm / dv0 - 1.0))
}

/// The pressure width `dv0`, or its combination with the Doppler width
/// `doppler` when the two are comparable (the Voigt half-width
/// approximation, with ln 2 written to four digits as in ATM).
#[expect(
    clippy::approx_constant,
    reason = "ATM's four-digit ln 2, kept for parity"
)]
fn doppler_combined(dv0: f64, doppler: f64) -> f64 {
    if dv0 / doppler < 1.25 {
        0.535 * dv0 + (0.217 * dv0.powf(2.0) + 0.6931 * doppler.powf(2.0)).sqrt()
    } else {
        dv0
    }
}
