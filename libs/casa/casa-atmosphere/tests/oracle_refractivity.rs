// SPDX-License-Identifier: LGPL-3.0-or-later
//! The species refractivities against ATM's `mkEarthSpecificRefractivity`:
//! all 25 opacity sources, at four (T, P, e) conditions spanning ATM's
//! three pressure regimes, over 431 frequencies that straddle every kind of
//! frequency-bin edge, line centre and cut-off ATM has.

mod common;

use casa_atmosphere::refractivity::{
    Species, density_refractivity, h2o_refractivity, o2_refractivity, specific_refractivity,
};
use common::{Agreement, f64_le, json, num, nums};
use num_complex::Complex64;

const REL: f64 = 1e-12;

/// A floor for one species and condition: 1e-13 of the largest magnitude
/// that species reaches over the frequencies.
///
/// The port and the C++ round differently in one place: libc++ divides
/// complex doubles through compiler-rt's `__divdc3`, which on Apple
/// silicon scales the operands and fuses its multiply-adds, while
/// `num-complex` divides without fusing. Each line's profile can then
/// differ by an ulp. That is invisible in absorption, but the delay (real
/// part) cancels between lines either side of a frequency, and there the
/// ulp shows as a larger relative error: the worst seen is 1.7e-12 on a
/// value 5.7e-7 of its species' peak, which is 5e-16 of the peak.
fn floor(values: &[Complex64]) -> f64 {
    1e-13
        * values
            .iter()
            .map(|v| v.re.abs().max(v.im.abs()))
            .fold(0.0, f64::max)
}

#[test]
fn species_refractivities_match_atm() {
    let meta = json("refractivity.json");
    let frequencies = nums(&meta["frequencies_ghz"]);
    let conditions = meta["conditions"].as_array().unwrap();
    let values = f64_le("refractivity.bin");
    // Every condition and frequency the oracle wrote (#894 review).
    assert_eq!((conditions.len(), frequencies.len()), (4, 431));
    assert_eq!(values.len(), conditions.len() * 25 * frequencies.len() * 2);
    let mut agreement = Agreement::new("species refractivity", REL);
    let mut at = 0;
    for condition in conditions {
        let (t, p, e) = (num(&condition[0]), num(&condition[1]), num(&condition[2]));
        for species in Species::ALL {
            let cpp: Vec<Complex64> = (0..frequencies.len())
                .map(|i| Complex64::new(values[at + 2 * i], values[at + 2 * i + 1]))
                .collect();
            at += 2 * frequencies.len();
            let floor = floor(&cpp);
            for (&nu, expected) in frequencies.iter().zip(&cpp) {
                let rust = specific_refractivity(species, t, p, e, nu);
                let place = || format!("{species:?} T={t} P={p} e={e} nu={nu}");
                agreement.check(rust.re, expected.re, floor, || format!("{} re", place()));
                agreement.check(rust.im, expected.im, floor, || format!("{} im", place()));
            }
        }
    }
    agreement.finish();
}

#[test]
fn layer_refractivities_match_atm() {
    let fixture = json("layer_refractivity.json");
    let o3 = num(&fixture["o3_per_m3"]);
    let co = num(&fixture["co_per_m3"]);
    let mut agreement = Agreement::new("layer refractivity", REL);
    // Four conditions at eight frequencies (#894 review).
    assert_eq!(fixture["rows"].as_array().unwrap().len(), 32, "layer rows");
    for row in fixture["rows"].as_array().unwrap() {
        let (t, p, e, nu) = (
            num(&row["t"]),
            num(&row["p"]),
            num(&row["e"]),
            num(&row["nu"]),
        );
        let rust = [
            o2_refractivity(Species::O2, t, p, e, nu),
            o2_refractivity(Species::O2Vibrational, t, p, e, nu),
            o2_refractivity(Species::O2Isotope18, t, p, e, nu),
            h2o_refractivity(Species::H2o, t, p, e, nu),
            h2o_refractivity(Species::H2oBending, t, p, e, nu),
            h2o_refractivity(Species::Hdo, t, p, e, nu),
            density_refractivity(Species::O3, t, p, nu, o3),
            density_refractivity(Species::Co, t, p, nu, co),
        ];
        let expected = row["values"].as_array().unwrap();
        assert_eq!(expected.len(), rust.len(), "a value a helper");
        for (k, (rust, cpp)) in rust.iter().zip(expected).enumerate() {
            let cpp = Complex64::new(num(&cpp[0]), num(&cpp[1]));
            let floor = 1e-13 * cpp.re.abs().max(cpp.im.abs());
            let place = || format!("helper {k} T={t} P={p} e={e} nu={nu}");
            agreement.check(rust.re, cpp.re, floor, || format!("{} re", place()));
            agreement.check(rust.im, cpp.im, floor, || format!("{} im", place()));
        }
    }
    agreement.finish();
}
