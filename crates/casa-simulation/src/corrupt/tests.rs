// SPDX-License-Identifier: LGPL-3.0-or-later
//! The simulator's keys: how a seed and an ordinal make a stream key, that
//! the same seed and ordinal draw the same values, that one seed adds
//! coherently and a repeated application independently, and that cells keep
//! the bits they had before the generator moved to casa-numerics (#855).
//! The draws' own quality, batching and accuracy are tested with the
//! generator, in `casa_numerics::cell_noise`.

use num_complex::Complex32;

use super::*;

/// Draws per statistic.
const N: usize = 1 << 20;

fn application(seed: i32, ordinal: u32) -> NoiseApplication {
    NoiseApplication {
        seed,
        level: NoiseLevel::Simple(1.0),
        ordinal,
    }
}

/// Add `noise`, a `simplenoise` application, to the cell of `row`.
fn add(
    noise: &NoiseApplication,
    row: u64,
    autocorrelation: bool,
    correlations: usize,
    cell: &mut [Complex32],
) {
    noise
        .uniform()
        .expect("a simplenoise application")
        .add_to_row(row, autocorrelation, correlations, cell);
}

/// The sample variance of `values` within five standard errors of
/// `expected`, the variance of the normal they are drawn from. It prints
/// what it measured, shown with `--nocapture`.
fn assert_variance(values: &[f64], expected: f64, what: &str) {
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
    let limit = 5.0 * expected * (2.0 / n).sqrt();
    println!("{what}: variance {variance:.5}, expected {expected} within {limit:.5}");
    assert!(
        (variance - expected).abs() < limit,
        "{what}: variance {variance:.5}, expected {expected} within {limit:.5}"
    );
}

/// The bits of every sample, (real, imaginary).
fn bits(cell: &[Complex32]) -> Vec<[u32; 2]> {
    cell.iter()
        .map(|sample| [sample.re.to_bits(), sample.im.to_bits()])
        .collect()
}

#[test]
fn the_key_is_the_seed_then_the_purpose_byte_above_the_ordinal() {
    assert_eq!(application(11_111, 3).key(), [11_111, 0x0100_0003]);
    assert_eq!(application(-1, 0).key(), [0xffff_ffff, 0x0100_0000]);
    assert_eq!(
        application(i32::MIN, MAXIMUM_APPLICATIONS - 1).key(),
        [0x8000_0000, 0x01ff_ffff]
    );
    let noise = NoiseApplication {
        seed: 7,
        level: NoiseLevel::Simple(0.25),
        ordinal: 2,
    }
    .uniform()
    .expect("a simplenoise application");
    assert_eq!((noise.key(), noise.sigma()), ([7, 0x0100_0002], 0.25));
}

#[test]
fn the_same_seed_and_ordinal_draw_the_same_values() {
    let first = application(42, 3);
    let second = application(42, 3);
    for row in [0, 7, 1 << 40] {
        let mut one = vec![Complex32::new(0.0, 0.0); 6 * 4];
        let mut other = one.clone();
        add(&first, row, false, 4, &mut one);
        add(&second, row, false, 4, &mut other);
        assert!(
            one.iter()
                .all(|sample| sample.re != 0.0 && sample.im != 0.0)
        );
        assert_eq!(bits(&one), bits(&other), "row {row}");
    }
}

/// Two terms with one seed add coherently (variance 4σ²); one term applied
/// twice, ordinals 0 and 1, adds independently (variance 2σ²).
#[test]
fn one_seed_adds_coherently_and_a_repeated_application_independently() {
    const CHANNELS: usize = 16;
    let (first, again, next) = (application(7, 0), application(7, 0), application(7, 1));
    let mut coherent = Vec::with_capacity(N);
    let mut repeated = Vec::with_capacity(N);
    for row in 0..(N / CHANNELS) as u64 {
        let mut same = [Complex32::new(0.0, 0.0); CHANNELS];
        add(&first, row, false, 1, &mut same);
        add(&again, row, false, 1, &mut same);
        let mut twice = [Complex32::new(0.0, 0.0); CHANNELS];
        add(&first, row, false, 1, &mut twice);
        add(&next, row, false, 1, &mut twice);
        coherent.extend(same.iter().map(|sample| f64::from(sample.re)));
        repeated.extend(twice.iter().map(|sample| f64::from(sample.im)));
    }
    assert_variance(&coherent, 4.0, "two terms of one seed");
    assert_variance(&repeated, 2.0, "one term applied twice");
}

/// The noise of three channels of four correlations of row 2³² + 5, under
/// seed 11111, ordinal 3 and σ = 0.1 Jy, bit for bit, (real, imaginary) by
/// channel then correlation, as computed on aarch64 when the draws moved to
/// single precision (#855). The generator, the counters, Box–Muller and the
/// scaling are integer and IEEE-754 arithmetic with nothing fused, so every
/// platform must give these bits; CI's x86-64 runners check that they do.
#[test]
fn a_known_cell_gets_its_known_bits_on_every_platform() {
    const KNOWN: [[u32; 2]; 12] = [
        [0x3e1d_fc8c, 0x3d90_052b],
        [0x3e16_de85, 0xbd21_c4b6],
        [0x3d6d_e0d4, 0x3da8_b85b],
        [0xbdbb_e1a5, 0x3e19_16ca],
        [0xbd4c_0290, 0xbb2e_ceba],
        [0x3dde_5ff2, 0x3d54_9052],
        [0xbd8e_d7be, 0xbe69_2d8d],
        [0xbde4_54d7, 0xbe62_ddf4],
        [0xbdc0_63e4, 0x3e26_71bb],
        [0x3d3a_0893, 0x3e1f_69f6],
        [0x3dfc_9901, 0x3da2_10b7],
        [0xbd8f_48c0, 0x3e07_8ae4],
    ];
    let noise = NoiseApplication {
        seed: 11_111,
        level: NoiseLevel::Simple(0.1),
        ordinal: 3,
    };
    let mut cell = vec![Complex32::new(0.0, 0.0); 3 * 4];
    add(&noise, (1 << 32) + 5, false, 4, &mut cell);
    assert_eq!(bits(&cell), KNOWN);
}

/// One cell of [`more_known_cells_keep_their_bits`].
struct KnownCell {
    noise: NoiseApplication,
    row: u64,
    autocorrelation: bool,
    correlations: usize,
    /// The cell's samples from zero, (real, imaginary) bits.
    bits: &'static [[u32; 2]],
}

/// Cells across the key's range, computed by the code in casa-simulation
/// before the generator moved to casa-numerics (#855), bit for bit, from
/// zero: the most negative seed with the largest ordinal on the last row,
/// eighteen channels of one correlation (a whole batch of sixteen and a
/// partial one); a negative seed on an autocorrelation row of two
/// correlations (real draws only); and three correlations (a pair and a
/// half) of two channels and one sample past them, which stays zero.
#[test]
fn more_known_cells_keep_their_bits() {
    let cells = [
        KnownCell {
            noise: NoiseApplication {
                seed: i32::MIN,
                level: NoiseLevel::Simple(2.5),
                ordinal: MAXIMUM_APPLICATIONS - 1,
            },
            row: u64::MAX,
            autocorrelation: false,
            correlations: 1,
            bits: &[
                [0x4024_a527, 0x3eaf_4aa3],
                [0xbfee_82ee, 0x3c85_f6b1],
                [0x3faa_e162, 0x3f2b_44de],
                [0xbecf_9b3a, 0xbfc4_be96],
                [0x3e2e_afee, 0x3f88_3ed9],
                [0x3f9d_6f2d, 0x3f72_f0b7],
                [0x4067_cf1f, 0xbfa0_4fd9],
                [0xc091_be4a, 0x4051_19c7],
                [0x3eb1_bb56, 0xbf8f_4e92],
                [0x3f46_77a1, 0xc01a_b30b],
                [0xbfdf_a9d0, 0xc08e_10a6],
                [0x4025_2965, 0xbf3c_5bc1],
                [0xbf99_fe92, 0xc05f_a4c4],
                [0x4012_cc1e, 0x4013_0f6d],
                [0xbf98_b0ab, 0xbde6_4b61],
                [0xc076_be1a, 0x3ffb_dbb4],
                [0x3f6b_922d, 0x401d_f4dc],
                [0xc006_b072, 0x3e55_f306],
            ],
        },
        KnownCell {
            noise: NoiseApplication {
                seed: -11_111,
                level: NoiseLevel::Simple(0.1),
                ordinal: 0,
            },
            row: 999,
            autocorrelation: true,
            correlations: 2,
            bits: &[
                [0x3e73_9ca3, 0x0000_0000],
                [0xbaa7_2316, 0x0000_0000],
                [0x3e0e_a8a2, 0x0000_0000],
                [0xbdae_c6dd, 0x0000_0000],
            ],
        },
        KnownCell {
            noise: NoiseApplication {
                seed: 0,
                level: NoiseLevel::Simple(3.7e-3),
                ordinal: 1,
            },
            row: (1 << 32) - 1,
            autocorrelation: false,
            correlations: 3,
            bits: &[
                [0x3b85_c330, 0xbb4d_d953],
                [0x3bbf_64e3, 0xbbfb_bf94],
                [0xbb11_abed, 0x3b96_5ca7],
                [0xba80_70f4, 0x3a43_31b5],
                [0xbb30_dc8e, 0x3b79_ff0c],
                [0x3bcc_9758, 0x3b64_505a],
                [0x0000_0000, 0x0000_0000],
            ],
        },
    ];
    for known in cells {
        let mut cell = vec![Complex32::new(0.0, 0.0); known.bits.len()];
        let (row, auto, correlations) = (known.row, known.autocorrelation, known.correlations);
        add(&known.noise, row, auto, correlations, &mut cell);
        assert_eq!(bits(&cell), known.bits, "{:?}, row {row}", known.noise);
    }
}
