// SPDX-License-Identifier: LGPL-3.0-or-later
use super::*;

#[test]
fn vla_bands_are_casas_open_intervals() {
    for (ghz, band) in [
        (0.05, Some(VlaBand::FourMetre)),
        (0.1, None),
        (0.3, Some(VlaBand::P)),
        (0.4, None),
        (1.0, None),
        (1.4, Some(VlaBand::L)),
        (2.0, None),
        (3.0, None),
        (5.0, Some(VlaBand::C)),
        (7.0, None),
        (8.4, Some(VlaBand::X)),
        (15.0, Some(VlaBand::U)),
        (22.0, Some(VlaBand::K)),
        (35.0, None),
        (45.0, Some(VlaBand::Q)),
        (55.0, None),
        (f64::NAN, None),
    ] {
        assert_eq!(VlaBand::of(ghz * 1.0e9), band, "{ghz} GHz");
    }
}

#[test]
fn evla_bands_take_the_first_interval_where_they_overlap() {
    for (mhz, band) in [
        (300.0, Some(EvlaBand::P)),
        (600.0, None),
        (1995.0, Some(EvlaBand::L)),
        (2002.0, Some(EvlaBand::L)),
        (2003.0, Some(EvlaBand::S)),
        (3995.0, Some(EvlaBand::S)),
        (4001.0, Some(EvlaBand::C)),
        (12000.5, Some(EvlaBand::X)),
        (12001.0, Some(EvlaBand::U)),
        (18500.0, None),
        (33000.0, Some(EvlaBand::A)),
        (45000.0, Some(EvlaBand::Q)),
        (50000.0, None),
    ] {
        assert_eq!(EvlaBand::of(mhz * 1.0e6), band, "{mhz} MHz");
    }
}

#[test]
fn the_evla_fit_is_the_nearest_within_the_band_the_higher_on_a_tie() {
    for (mhz, fit) in [
        // Nearest, and the higher of two equally near.
        (1400.0, 1424.0),
        (1392.0, 1424.0),
        (1391.0, 1360.0),
        // Limited within the band: L below 1040, S below 2052 and above
        // 3948, Q above 43948.
        (950.0, 1040.0),
        (2001.0, 2000.0),
        (2010.0, 2052.0),
        (3995.0, 3948.0),
        (49000.0, 43948.0),
        // CASA's map has no 11052: its second 10052 assignment won.
        (11100.0, 11180.0),
        // Between bands the frequency is kept, and the nearest fit of the
        // whole table taken: 18.5 GHz is as near 17948 as 19052.
        (18500.0, 19052.0),
        (700.0, 470.0),
        (60000.0, 43948.0),
    ] {
        assert_eq!(
            EvlaPowerPattern::at(mhz * 1.0e6).frequency_mhz(),
            fit,
            "{mhz} MHz"
        );
    }
    assert_eq!(
        EvlaPowerPattern::at(10.052e9).coefficients(),
        &[1.000, -1.401e-3, 7.12e-7, -1.31e-10]
    );
    // P band's fits have a fourth power.
    assert_eq!(EvlaPowerPattern::at(300.0e6).coefficients().len(), 5);
}

#[test]
fn every_evla_fit_is_its_own_frequencys_and_midpoints_take_the_higher() {
    // Within each band (between bands the limits, not the nearest rule,
    // decide): each fit's frequency picks it, and the midpoint of two
    // neighbours picks the higher, as `std::map::lower_bound` does.
    let fits = evla_coefficients::EVLA_COEFFICIENTS;
    for &(mhz, coefficients) in fits {
        let pattern = EvlaPowerPattern::at(mhz * 1.0e6);
        assert_eq!(pattern.frequency_mhz(), mhz);
        assert_eq!(pattern.coefficients(), coefficients);
    }
    for pair in fits.windows(2) {
        let [(low, _), (high, _)] = [pair[0], pair[1]];
        if EvlaBand::of(low * 1.0e6) != EvlaBand::of(high * 1.0e6) {
            continue;
        }
        let midpoint = f64::midpoint(low, high);
        for (mhz, expected) in [
            (midpoint - 0.001, low),
            (midpoint, high),
            (midpoint + 0.001, high),
        ] {
            assert_eq!(
                EvlaPowerPattern::at(mhz * 1.0e6).frequency_mhz(),
                expected,
                "{mhz} MHz"
            );
        }
    }
}

#[test]
fn the_evla_power_is_casas_sampled_table() {
    let pattern = EvlaPowerPattern::at(1.4e9);
    assert_eq!(pattern.power(0.0), 1.0);
    // The lower sample: just below a sample's radius takes the one before.
    let step = EvlaPowerPattern::MAXIMUM_RADIUS_ARCMIN_GHZ / 9_999.0;
    let at_sample = pattern.power(1_000.0 * step + 1.0e-9);
    assert_eq!(pattern.power(1_000.5 * step), at_sample);
    assert_ne!(pattern.power(1_000.0 * step - 1.0e-9), at_sample);
    // The half-power radius of the 1424 MHz fit is near 22 arcmin·GHz.
    let half = pattern.power(22.0);
    assert!((0.45..0.55).contains(&half), "{half}");
    // Beyond the null the polynomial is negative: zero, as CASA writes.
    assert_eq!(pattern.power(43.1), 0.0);
    assert_eq!(pattern.power(58.5), 0.0);
    assert_eq!(pattern.power(-1.0), 0.0);
    assert_eq!(pattern.power(f64::NAN), 0.0);
}
