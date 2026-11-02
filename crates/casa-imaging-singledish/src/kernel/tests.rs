// SPDX-License-Identifier: LGPL-3.0-or-later
//! The tables against `SDGrid::init`'s formulas, and the taps against
//! `ggridsd`'s placement. The tables' agreement with CASA's own is in
//! `tests/tsdimaging_casa.rs`, through the weight images it grids.

use ndarray::Array2;

use super::*;

fn table(function: GridFunction) -> KernelTable {
    KernelTable::new(function).expect("table")
}

#[test]
fn casas_parameters_resolve_to_its_defaults() {
    let default =
        |name: &str| GridFunction::from_casa(name, KernelParameters::DEFAULT).expect(name);
    assert_eq!(default("BOX"), GridFunction::Box { support: 0 });
    assert_eq!(default("sf"), GridFunction::Spheroidal { support: 3 });
    let ln2 = std::f64::consts::LN_2.sqrt();
    assert_eq!(
        default("Gauss"),
        GridFunction::Gauss {
            hwhm: ln2,
            truncate: (3.0 * ln2) as f32
        }
    );
    assert_eq!(
        default("GJINC"),
        GridFunction::Gjinc {
            hwhm: 2.52 * ln2,
            jwidth: 1.55,
            truncate: None
        }
    );
    assert_eq!(default("pb"), GridFunction::PrimaryBeam);
    let given = KernelParameters {
        convsupport: 6,
        truncate: 2.5,
        gwidth: 1.5,
        jwidth: 2.0,
    };
    assert_eq!(
        GridFunction::from_casa("SF", given).expect("sf"),
        GridFunction::Spheroidal { support: 6 }
    );
    assert_eq!(
        GridFunction::from_casa("GJINC", given).expect("gjinc"),
        GridFunction::Gjinc {
            hwhm: 1.5,
            jwidth: 2.0,
            truncate: Some(2.5)
        }
    );
    let zero = KernelParameters {
        convsupport: 0,
        ..KernelParameters::DEFAULT
    };
    assert!(GridFunction::from_casa("SF", zero).is_err());
    assert_eq!(
        GridFunction::from_casa("BOX", zero).expect("box"),
        GridFunction::Box { support: 0 }
    );
    assert!(GridFunction::from_casa("bessel", KernelParameters::DEFAULT).is_err());
    assert!(KernelTable::new(GridFunction::PrimaryBeam).is_err());
}

#[test]
fn box_is_one_out_to_a_pixel_past_its_support() {
    for support in [0, 2] {
        let kernel = table(GridFunction::Box { support });
        assert_eq!(kernel.support(), support);
        assert_eq!(kernel.values().len(), SAMPLING * (2 * support + 2));
        let ones = SAMPLING * (support + 1);
        assert!(kernel.values()[..ones].iter().all(|&v| v == 1.0));
        assert!(kernel.values()[ones..].iter().all(|&v| v == 0.0));
    }
}

#[test]
fn spheroidal_is_grdsf_tapered_over_its_support() {
    for support in [3, 6] {
        let kernel = table(GridFunction::Spheroidal { support });
        let samples = SAMPLING * support;
        for i in [0, 1, samples / 3, samples - 1] {
            let nu = i as f64 / samples as f64;
            assert_eq!(kernel.values()[i], ((1.0 - nu * nu) * grdsf(nu)) as f32);
        }
        assert!(kernel.values()[samples..].iter().all(|&v| v == 0.0));
    }
}

#[test]
fn gauss_is_a_half_width_gaussian_through_its_rounded_truncation() {
    let kernel = table(GridFunction::Gauss {
        hwhm: 1.0,
        truncate: 2.004,
    });
    // `⌈2.004⌉ = 3`; `itruncate = (Int)(200.4 + 0.5) = 200`, filled through it.
    assert_eq!(kernel.support(), 3);
    assert!((kernel.values()[100] - 0.5).abs() < 1.0e-7);
    assert!(kernel.values()[200] > 0.0);
    assert_eq!(kernel.values()[201], 0.0);
    let exact = table(GridFunction::Gauss {
        hwhm: 1.0,
        truncate: 2.0,
    });
    assert_eq!(exact.support(), 2);
}

#[test]
fn gjinc_ends_at_its_truncation_or_the_jincs_first_null() {
    let jwidth = 1.55;
    let null = table(GridFunction::Gjinc {
        hwhm: 2.0,
        jwidth,
        truncate: None,
    });
    // `⌈2c⌉ = 4`; J₁'s first zero is at 3.8317, so r = 3.8317 c / π.
    assert_eq!(null.support(), 4);
    let last = null
        .values()
        .iter()
        .rposition(|&v| v > 0.0)
        .expect("nonzero");
    let first_null = 3.831_705_97 * jwidth / std::f64::consts::PI;
    assert_eq!(last, (first_null * SAMPLING as f64) as usize);
    assert_eq!(null.values()[0], 1.0);
    // At r = 1: the Gaussian of half width 2, times the jinc.
    let x = std::f64::consts::PI / jwidth;
    let expected = 0.5_f64.powf(0.25) * 2.0 * libm::j1(x) / x;
    assert!((f64::from(null.values()[100]) - expected).abs() < 1.0e-7);

    // A truncation is filled up to, not through, `itruncate`.
    let cut = table(GridFunction::Gjinc {
        hwhm: 2.0,
        jwidth,
        truncate: Some(1.5),
    });
    assert_eq!(cut.support(), 2);
    assert!(cut.values()[149] > 0.0);
    assert_eq!(cut.values()[150], 0.0);
}

/// A Gaussian power pattern of `fwhm_rad` at every frequency.
struct Gaussian {
    fwhm_rad: f64,
    pixels: usize,
}

impl PowerPattern for Gaussian {
    fn power_on_axis(&self, offset_rad: f64, _frequency_hz: f64) -> f32 {
        (-4.0 * std::f64::consts::LN_2 * (offset_rad / self.fwhm_rad).powi(2)).exp() as f32
    }

    fn support_pixels(&self, _increment_x_rad: f64, _frequency_hz: f64) -> usize {
        self.pixels
    }
}

#[test]
fn the_primary_beam_kernel_keeps_one_sample_fewer_than_it_counts_above_the_cut() {
    let increment = 1.0e-5;
    let pattern = Gaussian {
        fwhm_rad: 10.0 * increment,
        pixels: 64,
    };
    let kernel = KernelTable::primary_beam(&pattern, increment, 1.0e11);
    let counted = (0..)
        .take_while(|&i| pattern.power_on_axis(f64::from(i) * increment / 100.0, 0.0) >= 1.0e-3)
        .count();
    assert_eq!(kernel.support(), counted / SAMPLING);
    let kept = kernel
        .values()
        .iter()
        .rposition(|&v| v > 0.0)
        .expect("kept")
        + 1;
    assert_eq!(kept, counted - 1);
    assert_eq!(kernel.values().len(), SAMPLING * (2 * kernel.support() + 2));
    assert!(GridFunction::PrimaryBeam.forming_bytes(64) >= kernel.bytes());
}

#[test]
fn forming_bytes_cover_each_table() {
    for function in [
        GridFunction::BOX,
        GridFunction::Spheroidal { support: 6 },
        GridFunction::Gauss {
            hwhm: 1.0,
            truncate: 3.2,
        },
        GridFunction::Gjinc {
            hwhm: 2.0,
            jwidth: 1.55,
            truncate: None,
        },
    ] {
        assert_eq!(
            function.forming_bytes(0),
            table(function).bytes(),
            "{function:?}"
        );
    }
}

#[test]
fn taps_take_the_nearest_pixel_and_the_truncated_radius() {
    let kernel = table(GridFunction::Spheroidal { support: 3 });
    let mut taps = Vec::new();
    // Zero-based (4.3, 5.75): one-based (5.3, 6.75), nearest (5, 7), offsets
    // (−30, 25) hundredths.
    assert!(kernel.taps([4.3, 5.75], (10, 10), &mut taps));
    assert_eq!(taps.len(), 49);
    assert_eq!((taps[0].x, taps[0].y), (1, 3));
    assert_eq!((taps[1].x, taps[1].y), (2, 3));
    let centre = taps
        .iter()
        .find(|tap| (tap.x, tap.y) == (4, 6))
        .expect("nearest pixel");
    let radius = ((30.0_f32 * 30.0 + 25.0 * 25.0).sqrt()) as usize;
    assert_eq!(centre.weight, kernel.values()[radius]);
    // Near the edge: only the pixels on the image.
    taps.clear();
    assert!(kernel.taps([0.2, 0.2], (10, 10), &mut taps));
    assert_eq!(taps.len(), 16);
    // The nearest pixel off the image: none, though the support reaches it.
    taps.clear();
    assert!(!kernel.taps([-0.6, 4.0], (10, 10), &mut taps));
    assert!(taps.is_empty());
    assert!(kernel.taps([-0.4, 4.0], (10, 10), &mut taps));
}

#[test]
fn degridding_sums_the_taps_over_the_plane() {
    let kernel = table(GridFunction::Spheroidal { support: 3 });
    let plane = Array2::from_shape_fn((12, 10), |(y, x)| (x + 10 * y) as f32);
    let mut taps = Vec::new();
    kernel.taps([4.6, 6.2], (10, 12), &mut taps);
    let expected: f32 = taps
        .iter()
        .fold(0.0, |sum, tap| sum + tap.weight * plane[[tap.y, tap.x]]);
    assert_eq!(kernel.degrid(plane.view(), [4.6, 6.2]), expected);
    assert_eq!(kernel.degrid(plane.view(), [-3.0, 6.2]), 0.0);
}
