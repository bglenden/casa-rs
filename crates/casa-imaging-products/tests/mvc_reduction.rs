// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA's streamed cube-to-Taylor normal image conversion (#1069).

use casa_imaging_products::TaylorMomentAccumulator;

#[test]
fn unequal_cube_channels_reduce_to_weighted_taylor_moments() {
    let common_pb = [11.0_f32 / 15.0, 0.05];
    let mut psf = vec![99.0_f32; 6];
    let mut residual = vec![99.0_f32; 4];
    let mut sumwt = [99.0_f32; 3];
    let mut reducer =
        TaylorMomentAccumulator::new(&common_pb, &mut psf, &mut residual, &mut sumwt, 100.0, 0.2)
            .unwrap();
    reducer
        .add(&[1.0, 99.0], &[2.0, 9.0], &[1.0, 0.3], 1.0, 80.0)
        .unwrap();
    reducer
        .add(&[0.5, 99.0], &[1.0, 9.0], &[0.8, 0.0], 2.0, 100.0)
        .unwrap();
    reducer
        .add(&[0.25, 99.0], &[-1.0, 9.0], &[0.6, 0.0], 3.0, 120.0)
        .unwrap();
    // A zero-weight channel cannot alter the result.
    reducer
        .add(&[10.0, 10.0], &[10.0, 10.0], &[1.0, 1.0], 0.0, 130.0)
        .unwrap();
    reducer.finish().unwrap();
    for (actual, expected) in
        psf.as_chunks::<2>()
            .0
            .iter()
            .zip([2.25 / 4.4, -0.11 / 4.4, 0.058 / 4.4])
    {
        assert!((actual[0] - expected).abs() < 1e-6, "PSF {actual:?}");
        assert_eq!(actual[1], 0.0, "mean PB below the strict cutoff");
    }
    for (actual, expected) in residual
        .as_chunks::<2>()
        .0
        .iter()
        .zip([1.8 / 4.4, -0.76 / 4.4])
    {
        assert!((actual[0] - expected).abs() < 1e-6, "residual {actual:?}");
        assert_eq!(actual[1], 0.0);
    }
    for (actual, expected) in sumwt.iter().zip([14.0 / 6.0, 1.6 / 6.0, 0.4 / 6.0]) {
        assert!((actual - expected).abs() < 1e-6, "sumwt {actual}");
    }
}

#[test]
fn invalid_taylor_reductions_are_rejected_and_partial_results_poisoned() {
    for mean in [
        vec![],
        vec![0.0],
        vec![-0.1],
        vec![f32::NAN],
        vec![0.5, -0.1],
        vec![0.5, f32::NAN],
    ] {
        let mut psf = vec![0.0; mean.len() * 3];
        let mut residual = vec![0.0; mean.len() * 2];
        let mut sumwt = [0.0; 3];
        assert!(
            TaylorMomentAccumulator::new(&mean, &mut psf, &mut residual, &mut sumwt, 100.0, 0.2)
                .is_err()
        );
    }
    let common = [0.5];
    for (psf_len, residual_len, reference, cutoff) in [
        (2, 2, 100.0, 0.2),
        (3, 1, 100.0, 0.2),
        (3, 2, 0.0, 0.2),
        (3, 2, f64::NAN, 0.2),
        (3, 2, 100.0, -0.1),
        (3, 2, 100.0, f32::NAN),
    ] {
        let mut psf = vec![0.0; psf_len];
        let mut residual = vec![0.0; residual_len];
        let mut sumwt = [0.0; 3];
        assert!(
            TaylorMomentAccumulator::new(
                &common,
                &mut psf,
                &mut residual,
                &mut sumwt,
                reference,
                cutoff
            )
            .is_err()
        );
    }
    let bad_channels = [
        (vec![], vec![1.0], vec![1.0], 1.0, 100.0),
        (vec![1.0], vec![], vec![1.0], 1.0, 100.0),
        (vec![1.0], vec![1.0], vec![], 1.0, 100.0),
        (vec![1.0], vec![1.0], vec![1.0], -1.0, 100.0),
        (vec![1.0], vec![1.0], vec![1.0], f32::NAN, 100.0),
        (vec![1.0], vec![1.0], vec![1.0], 1.0, 0.0),
        (vec![1.0], vec![1.0], vec![1.0], 1.0, f64::NAN),
        (vec![1.0], vec![1.0], vec![1.0], 1.0, f64::MAX),
        (vec![1.0], vec![1.0], vec![-1.0], 1.0, 100.0),
        (vec![1.0], vec![1.0], vec![f32::NAN], 1.0, 100.0),
        (vec![1.0], vec![1.0], vec![0.2], 1.0, 100.0),
        (vec![f32::NAN], vec![1.0], vec![1.0], 1.0, 100.0),
        (vec![1.0], vec![f32::NAN], vec![1.0], 1.0, 100.0),
        (vec![1.0], vec![1.0], vec![1.0], f32::MAX, 100.0),
    ];
    for (psf, residual, pb, weight, frequency) in bad_channels {
        let mut psf_out = [0.0; 3];
        let mut residual_out = [0.0; 2];
        let mut sumwt = [0.0; 3];
        let mut reducer = TaylorMomentAccumulator::new(
            &common,
            &mut psf_out,
            &mut residual_out,
            &mut sumwt,
            100.0,
            0.2,
        )
        .unwrap();
        reducer.add(&[1.0], &[1.0], &[1.0], 1.0, 100.0).unwrap();
        assert!(
            reducer
                .add(&psf, &residual, &pb, weight, frequency)
                .is_err()
        );
        assert!(reducer.add(&[1.0], &[1.0], &[1.0], 1.0, 100.0).is_err());
        assert!(reducer.finish().is_err());
    }
    let mut psf = [0.0; 3];
    let mut residual = [0.0; 2];
    let mut sumwt = [0.0; 3];
    let reducer =
        TaylorMomentAccumulator::new(&common, &mut psf, &mut residual, &mut sumwt, 100.0, 0.2)
            .unwrap();
    assert!(reducer.finish().is_err(), "zero total weight");
    let mut psf = [0.0; 3];
    let mut residual = [0.0; 2];
    let mut sumwt = [0.0; 3];
    let reducer =
        TaylorMomentAccumulator::new(&[0.1], &mut psf, &mut residual, &mut sumwt, 100.0, 0.2)
            .unwrap();
    assert!(
        reducer.finish().is_err(),
        "zero total weight even when every image pixel is blanked"
    );
    let mut psf = [0.0; 3];
    let mut residual = [0.0; 2];
    let mut sumwt = [0.0; 3];
    let mut reducer = TaylorMomentAccumulator::new(
        &[f32::MIN_POSITIVE],
        &mut psf,
        &mut residual,
        &mut sumwt,
        100.0,
        0.0,
    )
    .unwrap();
    reducer
        .add(&[f32::MAX / 2.0], &[1.0], &[1.0], 1.0, 100.0)
        .unwrap();
    assert!(reducer.finish().is_err(), "normalization overflow");
}

/// Four actual CASA 6.7.6.14 pixels from the unequal-weight, wide-field
/// synthetic fixture, including a nonzero Taylor PB below its mask cutoff.
#[test]
fn cube_moment_reduction_matches_casa_float_products() {
    let mean_pb = [1.0, 0.573_425_1, 0.729_342_1, 0.047_247_656];
    let channel_pb = [
        [1.0, 0.574_869_1, 0.730_340_54, 0.101_429_46],
        [1.0, 0.572_862_7, 0.728_953_9, 0.0],
        [1.0, 0.570_856_6, 0.727_565_05, 0.0],
    ];
    let channel_psf = [
        [1.0, -0.010_330_508, 0.058_767_933, 0.023_586_003],
        [1.0, 0.000_478_237_95, 0.067_291_88, 0.024_339_762],
        [1.0, 0.011_271_036, 0.073_475_68, 0.021_031_333],
    ];
    let channel_residual = [
        [-0.269_359_68, 0.097_355_455, -0.134_928_54, 0.016_565_677],
        [-0.247_256_58, 0.068_877_88, -0.091_016_26, 0.0],
        [-0.226_721_96, 0.038_695_51, -0.043_917_78, 0.0],
    ];
    let mut psf = [0.0; 12];
    let mut residual = [0.0; 8];
    let mut sumwt = [0.0; 3];
    let mut reducer =
        TaylorMomentAccumulator::new(&mean_pb, &mut psf, &mut residual, &mut sumwt, 45e9, 0.1)
            .unwrap();
    for channel in 0..3 {
        reducer
            .add(
                &channel_psf[channel],
                &channel_residual[channel],
                &channel_pb[channel],
                [10_529.967, 7_880.975, 4_194.388][channel],
                [44.0e9, 44.128e9, 44.256e9][channel],
            )
            .unwrap();
    }
    reducer.finish().unwrap();
    let expected_psf = [
        [1.0, -0.002_575_729_7, 0.064_460_434, 0.0],
        [-0.020_174_99, 0.000_069_556_24, -0.001_288_389_1, 0.0],
        [
            0.000_411_664_72,
            -0.000_001_750_696_7,
            0.000_026_059_279,
            0.0,
        ],
    ];
    let expected_residual = [
        [-0.253_742_43, 0.076_601_43, -0.102_781_62, 0.0],
        [0.005_154_204, -0.001_593_354_3, 0.002_147_581_2, 0.0],
    ];
    for (actual, expected) in psf.iter().zip(expected_psf.into_iter().flatten()) {
        assert!(
            (actual - expected).abs() < 1e-6,
            "PSF {actual} vs {expected}"
        );
    }
    for (actual, expected) in residual.iter().zip(expected_residual.into_iter().flatten()) {
        assert!(
            (actual - expected).abs() < 1e-6,
            "residual {actual} vs {expected}"
        );
    }
    for (actual, expected) in sumwt.iter().zip([8_430.882, -175.110_17, 3.666_693]) {
        assert!(
            (actual - expected).abs() < 5e-5 * expected.abs().max(1.0),
            "sumwt {actual} vs {expected}"
        );
    }
}

#[test]
fn overflowing_psf_or_residual_accumulation_poisons_the_reducer() {
    for psf_overflows in [true, false] {
        let mut psf = [0.0; 3];
        let mut residual = [0.0; 2];
        let mut sumwt = [0.0; 3];
        let mut reducer =
            TaylorMomentAccumulator::new(&[0.5], &mut psf, &mut residual, &mut sumwt, 100.0, 0.2)
                .unwrap();
        let huge = f32::MAX * 0.75;
        let (p, r) = if psf_overflows {
            (huge, 1.0)
        } else {
            (1.0, huge)
        };
        reducer.add(&[p], &[r], &[1.0], 1.0, 100.0).unwrap();
        assert!(reducer.add(&[p], &[r], &[1.0], 1.0, 100.0).is_err());
        assert!(reducer.finish().is_err());
    }
}

#[test]
fn taylor_image_support_is_strict_at_the_mean_pb_cutoff() {
    let cutoff = 0.2_f32;
    let above = f32::from_bits(cutoff.to_bits() + 1);
    let mean = [cutoff, above];
    let mut psf = [0.0; 6];
    let mut residual = [0.0; 4];
    let mut sumwt = [0.0; 3];
    let mut reducer =
        TaylorMomentAccumulator::new(&mean, &mut psf, &mut residual, &mut sumwt, 100.0, cutoff)
            .unwrap();
    reducer
        .add(&[1.0, 1.0], &[1.0, 1.0], &[0.4, above * 2.0], 1.0, 100.0)
        .unwrap();
    reducer
        .add(&[1.0, 1.0], &[1.0, 1.0], &[0.0, 0.0], 1.0, 100.0)
        .unwrap();
    reducer.finish().unwrap();
    assert_eq!(psf[0], 0.0);
    assert!((psf[1] - 1.0).abs() < 1e-6);
    assert_eq!(residual[0], 0.0);
    assert!((residual[1] - 1.0).abs() < 1e-6);
}

#[test]
fn mvc_normal_encoding_moves_normalized_planes_without_losing_extreme_values() {
    use casa_imaging_products::encode_mvc_taylor_planes;
    let psf = vec![1.0_f32, 2.0, 3.0];
    let residual = vec![4.0_f32, 5.0];
    let psf_ptr = psf.as_ptr();
    let residual_ptr = residual.as_ptr();
    let images = encode_mvc_taylor_planes([1, 1], psf, residual, [2.0, -1.0, 0.5]).unwrap();
    assert_eq!(images.psf.as_ref().unwrap().as_ptr(), psf_ptr);
    assert_eq!(images.residual.as_ptr(), residual_ptr);
    assert_eq!(images.psf.as_ref().unwrap(), &[1.0, 2.0, 3.0]);
    assert_eq!(images.residual, [4.0, 5.0]);
    assert_eq!(images.sum_weights, [1.0, -0.5, 0.25]);
    assert_eq!(images.published_sum_weights, [2.0, -1.0, 0.5]);
    let cases = [
        ([0, 1], vec![], vec![], [1.0, 0.0, 0.0]),
        ([usize::MAX, 2], vec![], vec![], [1.0, 0.0, 0.0]),
        ([1, 1], vec![1.0; 2], vec![1.0; 2], [1.0, 0.0, 0.0]),
        ([1, 1], vec![1.0; 3], vec![1.0], [1.0, 0.0, 0.0]),
        ([1, 1], vec![1.0; 3], vec![1.0; 2], [0.0, 0.0, 0.0]),
        ([1, 1], vec![1.0; 3], vec![1.0; 2], [-1.0, 0.0, 0.0]),
        ([1, 1], vec![1.0; 3], vec![1.0; 2], [1.0, f32::NAN, 0.0]),
        (
            [1, 1],
            vec![f32::NAN, 1.0, 1.0],
            vec![1.0; 2],
            [1.0, 0.0, 0.0],
        ),
        ([1, 1], vec![1.0; 3], vec![f32::NAN, 1.0], [1.0, 0.0, 0.0]),
    ];
    for (shape, psf, residual, weights) in cases {
        assert!(encode_mvc_taylor_planes(shape, psf, residual, weights).is_err());
    }
    let tiny = f32::from_bits(1);
    let extremes = encode_mvc_taylor_planes(
        [1, 1],
        vec![f32::MAX, tiny, 1.0],
        vec![tiny, f32::MAX],
        [2.0, 0.0, 0.0],
    )
    .unwrap();
    assert_eq!(extremes.psf.unwrap(), [f32::MAX, tiny, 1.0]);
    assert_eq!(extremes.residual, [tiny, f32::MAX]);
}

/// An MVC all-zero Float Taylor family moves its planes without inventing a
/// positive published weight; any nonblank or invalid input is refused.
#[test]
fn mvc_blank_normal_state_accepts_only_exact_zero_moments() {
    use casa_imaging_products::encode_mvc_blank_taylor_planes;
    let psf = vec![0.0_f32; 6];
    let residual = vec![0.0_f32; 4];
    let psf_pointer = psf.as_ptr();
    let residual_pointer = residual.as_ptr();
    let images = encode_mvc_blank_taylor_planes([2, 1], psf, residual, [0.0, -0.0, 0.0]).unwrap();
    assert_eq!(images.psf.as_ref().unwrap().as_ptr(), psf_pointer);
    assert_eq!(images.residual.as_ptr(), residual_pointer);
    assert_eq!(images.sum_weights, [1.0, 0.0, 0.0]);
    assert_eq!(images.published_sum_weights, [0.0, 0.0, 0.0]);
    let cases = [
        ([0, 1], vec![], vec![], [0.0; 3]),
        ([usize::MAX, 2], vec![], vec![], [0.0; 3]),
        ([1, 1], vec![0.0; 2], vec![0.0; 2], [0.0; 3]),
        ([1, 1], vec![0.0; 3], vec![0.0], [0.0; 3]),
        (
            [1, 1],
            vec![0.0; 3],
            vec![0.0; 2],
            [f32::from_bits(1), 0.0, 0.0],
        ),
        ([1, 1], vec![0.0; 3], vec![0.0; 2], [0.0, f32::NAN, 0.0]),
        (
            [1, 1],
            vec![0.0, f32::from_bits(1), 0.0],
            vec![0.0; 2],
            [0.0; 3],
        ),
        ([1, 1], vec![0.0, f32::NAN, 0.0], vec![0.0; 2], [0.0; 3]),
        ([1, 1], vec![0.0; 3], vec![f32::from_bits(1), 0.0], [0.0; 3]),
        ([1, 1], vec![0.0; 3], vec![f32::INFINITY, 0.0], [0.0; 3]),
    ];
    for (shape, psf, residual, weights) in cases {
        assert!(encode_mvc_blank_taylor_planes(shape, psf, residual, weights).is_err());
    }
}
