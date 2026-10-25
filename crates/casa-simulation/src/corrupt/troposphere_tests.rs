// SPDX-License-Identifier: LGPL-3.0-or-later
//! The tropospheric screens: CASA's layout, the normalisation, frozen flow
//! and an isotropic spectrum.

use num_complex::Complex32;

use super::{ScreenLayout, TroposphericPhase, antenna_series, screen_excess};
use crate::corrupt::jones::Slots;

/// The six ALMA antennas (`alma.cycle10.3.cfg`) of the Phase 4d `screen`
/// probe, ITRF metres.
const PROBE_ANTENNAS: [[f64; 3]; 6] = [
    [
        2_225_004.468_449_519_5,
        -5_440_060.207_407_018,
        -2_481_684.920_604_503,
    ],
    [
        2_225_020.150_904_546,
        -5_440_055.122_577_036,
        -2_481_682.028_548_035,
    ],
    [
        2_225_052.376_592_874_5,
        -5_440_045.715_534_717,
        -2_481_673.806_727_262_7,
    ],
    [
        2_225_048.278_898_055,
        -5_440_052.740_630_065,
        -2_481_661.134_408_941,
    ],
    [
        2_225_025.227_590_547_4,
        -5_440_076.621_036_539,
        -2_481_630.401_557_795_7,
    ],
    [
        2_225_067.373_191_722_7,
        -5_440_024.553_751_563,
        -2_481_706.142_659_697_7,
    ],
];

fn mean_and_rms(values: &[f64]) -> (f64, f64) {
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64;
    (mean, variance.sqrt())
}

/// The probe's array of 12 m dishes, a 7 m/s wind over its 1,098 s track:
/// CASA 6.7.6 logged "creating new fBM of size 1359,271", a screen widened
/// to a fifth of its length.
#[test]
fn the_screen_is_laid_out_as_casas() {
    let layout = ScreenLayout::new(&PROBE_ANTENNAS, &[12.0; 6], (7.0, 1_098.0)).expect("layout");
    assert_eq!(layout.pixel_m, 6.0);
    assert_eq!(layout.shape, [271, 1_359]);
    assert!(
        layout
            .antennas
            .iter()
            .all(|&[row, column]| row < 20 && column < 20),
        "{:?}",
        layout.antennas
    );
    // Without wind the screen is the array's extent and the border; a
    // screen no longer than five times its width is not widened.
    let still = ScreenLayout::new(&PROBE_ANTENNAS, &[12.0; 6], (0.0, 1_098.0)).expect("layout");
    assert!(still.shape[1] <= 5 * still.shape[0], "{:?}", still.shape);
    // A dish of no size, or a wind's run past what a screen can hold, is
    // refused rather than made or indexed beyond (#899 review).
    assert!(ScreenLayout::new(&PROBE_ANTENNAS, &[0.0; 6], (7.0, 1_098.0)).is_err());
    assert!(ScreenLayout::new(&PROBE_ANTENNAS, &[12.0; 6], (1e30, 1_098.0)).is_err());
    assert!(ScreenLayout::new(&PROBE_ANTENNAS, &[12.0; 6], (1e9, 1_098.0)).is_err());
}

/// The screen's spectrum is Hermitian where the real transform needs it to
/// be: on the half spectrum's first column, row r is row N − r's
/// conjugate. Drawn apart, those modes would come out of the transform at
/// half the power of modes in its interior at the same wavenumber (#899
/// review: 0.50 measured). On a square screen the modes (r, 0) and (0, r)
/// share a wavenumber; over many screens their powers agree.
#[test]
fn boundary_modes_have_the_power_of_interior_modes() {
    let side = 32;
    let layout = ScreenLayout {
        pixel_m: 1.0,
        shape: [side, side],
        antennas: (0..side)
            .flat_map(|row| (0..side).map(move |column| [row, column]))
            .collect(),
    };
    let slots = Slots {
        start_s: 0.0,
        width_s: 1.0,
        count: 1,
    };
    let mut fft = casa_fft::RealFft2::<f64>::with_threads([side, side], 1)
        .expect("transform")
        .with_estimated_plan();
    let half = side / 2 + 1;
    // Each wavenumber's power over the screens, boundary and interior: a
    // mode's power is exponentially distributed, so each ratio of 2,048 has
    // a standard error of about 3 %, and their mean of about 1 %.
    let mut boundary = vec![0.0; side / 2];
    let mut interior = vec![0.0; side / 2];
    for seed in 0..2_048 {
        let screen = screen_excess(seed, (2.0, 1.0), &layout, (&slots, 0.0), 1).expect("screen");
        let mut plane = vec![num_complex::Complex64::new(0.0, 0.0); side * half];
        for (row, values) in plane.chunks_exact_mut(half).enumerate() {
            for column in 0..side {
                let value = f64::from(screen[row * side + column]);
                let pair = &mut values[column / 2];
                if column.is_multiple_of(2) {
                    pair.re = value;
                } else {
                    pair.im = value;
                }
            }
        }
        fft.forward(&mut plane).expect("forward");
        for wavenumber in 1..side / 2 {
            boundary[wavenumber] += plane[wavenumber * half].norm_sqr();
            interior[wavenumber] += plane[wavenumber].norm_sqr();
        }
    }
    let ratios: Vec<f64> = (1..side / 2)
        .map(|wavenumber| boundary[wavenumber] / interior[wavenumber])
        .collect();
    let ratio = ratios.iter().sum::<f64>() / ratios.len() as f64;
    assert!(
        (0.95..1.05).contains(&ratio),
        "boundary/interior power {ratio} ({ratios:?})"
    );
}

/// Each antenna's series is scaled to the root-mean-square asked for about
/// its own mean, which is kept; antennas and seeds draw apart.
#[test]
fn individual_series_are_scaled_to_deltapwv_about_their_means() {
    let (slots, antennas) = (120, 4);
    let excess = antenna_series(7, (1.1, 0.15), slots, antennas);
    let series = |excess: &[f32], antenna: usize| -> Vec<f64> {
        (0..slots)
            .map(|slot| f64::from(excess[slot * antennas + antenna]))
            .collect()
    };
    let mut means = Vec::new();
    for antenna in 0..antennas {
        let (mean, rms) = mean_and_rms(&series(&excess, antenna));
        assert!((rms - 0.15).abs() < 1e-6, "antenna {antenna}: rms {rms}");
        means.push(mean);
    }
    assert!(means.iter().any(|mean| mean.abs() > 1e-3), "{means:?}");
    assert_ne!(series(&excess, 0), series(&excess, 1));
    assert_eq!(excess, antenna_series(7, (1.1, 0.15), slots, antennas));
    assert_ne!(excess, antenna_series(8, (1.1, 0.15), slots, antennas));
}

/// The screen moves with the wind: an antenna the wind's run in one slot
/// upwind of another reads in slot `s + 1` what the other read in slot
/// `s`, and without wind each antenna reads one pixel throughout.
#[test]
fn the_screen_is_frozen_and_blown_downwind() {
    let slots = Slots {
        start_s: 0.0,
        width_s: 8.0,
        count: 12,
    };
    // 6 m pixels and 1.5 m/s: two pixels a slot.
    let layout = ScreenLayout {
        pixel_m: 6.0,
        shape: [40, 64],
        antennas: vec![[5, 3], [5, 5], [9, 3]],
    };
    let excess = screen_excess(3, (1.5, 0.2), &layout, (&slots, 1.5), 1).expect("screen");
    for slot in 0..slots.count - 1 {
        assert_eq!(excess[(slot + 1) * 3], excess[slot * 3 + 1], "slot {slot}");
    }
    assert_ne!(excess[0], excess[2], "rows differ");
    let still = screen_excess(3, (1.5, 0.2), &layout, (&slots, 0.0), 1).expect("screen");
    assert!((0..slots.count).all(|slot| still[slot * 3] == still[0]));
    // One screen whatever the transform's threads.
    assert_eq!(
        excess,
        screen_excess(3, (1.5, 0.2), &layout, (&slots, 1.5), 4).expect("screen")
    );
}

/// The screen's root-mean-square over every pixel is `deltapwv`, and its
/// spectrum is isotropic on a screen five times as long as wide: the
/// structure function at one lag is the same along either axis. (CASA's
/// spectrum, in index units on unwrapped indices, is not.)
#[test]
fn the_screen_is_scaled_to_deltapwv_and_isotropic() {
    let [rows, columns] = [128, 640];
    let lag = 4;
    let slots = Slots {
        start_s: 0.0,
        width_s: 1.0,
        count: 1,
    };
    let mut along = 0.0;
    let mut across = 0.0;
    let mut pixels = Vec::new();
    for seed in 0..8 {
        // Read every pixel by making each one an antenna.
        let antennas: Vec<[usize; 2]> = (0..rows)
            .flat_map(|row| (0..columns).map(move |column| [row, column]))
            .collect();
        let layout = ScreenLayout {
            pixel_m: 1.0,
            shape: [rows, columns],
            antennas,
        };
        let screen = screen_excess(seed, (2.0, 0.3), &layout, (&slots, 0.0), 2).expect("screen");
        let pixel = |row: usize, column: usize| f64::from(screen[row * columns + column]);
        let (mut x, mut y, mut count) = (0.0, 0.0, 0.0);
        for row in 0..rows - lag {
            for column in 0..columns - lag {
                x += (pixel(row, column + lag) - pixel(row, column)).powi(2);
                y += (pixel(row + lag, column) - pixel(row, column)).powi(2);
                count += 1.0;
            }
        }
        along += x / count;
        across += y / count;
        pixels.extend(screen.iter().map(|&value| f64::from(value)));
        let (_, rms) = mean_and_rms(&screen.iter().map(|&v| f64::from(v)).collect::<Vec<_>>());
        assert!((rms - 0.3).abs() < 1e-5, "seed {seed}: rms {rms}");
    }
    let ratio = along / across;
    assert!((0.85..1.18).contains(&ratio), "along/across {ratio}");
}

/// A solution's phasors are `exp(i·δ·φ(c))`.
#[test]
fn phasors_turn_the_excess_through_each_channels_delay() {
    let phase = TroposphericPhase::new(vec![0.1, -0.2, 0.3, 0.4], vec![Some(vec![1.0, 20.0])]);
    let mut phasors = [Complex32::new(0.0, 0.0); 2];
    phase.phasors((0, 1), (0, 2), &mut phasors);
    let delta = f64::from(0.3_f32);
    for (phasor, delay) in phasors.iter().zip([1.0, 20.0]) {
        let (sin, cos) = (delta * delay).sin_cos();
        assert_eq!(*phasor, Complex32::new(cos as f32, sin as f32));
    }
    assert_eq!(phase.channels(0), Some(2));
    assert_eq!(phase.channels(1), None);
}
