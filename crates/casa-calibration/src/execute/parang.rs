// SPDX-License-Identifier: LGPL-3.0-or-later
//! PJones::calcOneJones: diagonal circular gains or a full linear-feed rotation.

use super::*;

pub(crate) fn apply(
    data_desc_id: i32,
    correlations: &[i32],
    first_angle: f64,
    second_angle: f64,
    corrected: &mut ArrayD<Complex32>,
) -> Result<(), ApplyExecutionError> {
    if correlations.iter().all(|code| matches!(code, 5..=8)) {
        for (corr, &code) in correlations.iter().enumerate() {
            let gain = parallactic_angle_gain(
                code,
                data_desc_id,
                correlations,
                first_angle,
                second_angle,
            )?;
            corrected
                .index_axis_mut(ndarray::Axis(0), corr)
                .mapv_inplace(|value| value / gain);
        }
        return Ok(());
    }
    let indices = [9, 10, 11, 12].map(|code| correlations.iter().position(|&value| value == code));
    let error = || ApplyExecutionError::UnsupportedParallacticAngleBasis {
        data_desc_id,
        correlation_types: correlations
            .iter()
            .map(|&code| stokes_name(code).to_string())
            .collect(),
    };
    if correlations.len() != 4 {
        return Err(error());
    }
    let [Some(xx), Some(xy), Some(yx), Some(yy)] = indices else {
        return Err(error());
    };
    let rotations = [first_angle, second_angle].map(|angle| {
        // PJones stores the feed angle as Float and constructs its Jones
        // matrix from the argument of a single-precision unit phasor.
        let (sin, cos) = f64::from(angle as f32).sin_cos();
        (sin as f32).atan2(cos as f32).sin_cos()
    });
    for channel in 0..corrected.shape()[1] {
        let indices = [xx, xy, yx, yy];
        let data = indices.map(|index| corrected[[index, channel]]);
        let rotated = rotate_linear(data, rotations);
        for (index, value) in indices.into_iter().zip(rotated) {
            corrected[[index, channel]] = value;
        }
    }
    Ok(())
}

/// Corrupt a model with the two feed Jones matrices in CASA's order.
pub(super) fn corrupt(
    data_desc_id: i32,
    correlations: &[i32],
    first_angle: f64,
    second_angle: f64,
    model: &mut ArrayD<Complex32>,
) -> Result<(), ApplyExecutionError> {
    if !correlations.iter().all(|code| matches!(code, 5..=8)) {
        return apply(
            data_desc_id,
            correlations,
            -first_angle,
            -second_angle,
            model,
        );
    }
    // PJones rounds each feed angle to Float before taking its phasor.
    // JonesDiag then multiplies the model by the left matrix and the
    // conjugate right matrix separately, rounding at each antenna.
    let [left, right] =
        [first_angle, second_angle].map(|angle| circular_parang_gain(f64::from(angle as f32)));
    for (index, correlation) in correlations.iter().enumerate() {
        let (first, second) = match correlation {
            5 => (left.conj(), right),
            6 => (left.conj(), right.conj()),
            7 => (left, right),
            8 => (left, right.conj()),
            _ => unreachable!("circular correlations"),
        };
        model
            .index_axis_mut(ndarray::Axis(0), index)
            .mapv_inplace(|value| (value * first) * second);
    }
    Ok(())
}

/// Correction for two real Jones matrices with rows [cos(a), sin(a)] and
/// [-sin(a), cos(a)] uses transpose(P1) * V * P2.
pub(super) fn rotate_linear(
    [xx, xy, yx, yy]: [Complex32; 4],
    [(s1, c1), (s2, c2)]: [(f32, f32); 2],
) -> [Complex32; 4] {
    let (a, b) = (xx * c1 - yx * s1, xy * c1 - yy * s1);
    let (c, d) = (xx * s1 + yx * c1, xy * s1 + yy * c1);
    [
        a * c2 - b * s2,
        a * s2 + b * c2,
        c * c2 - d * s2,
        c * s2 + d * c2,
    ]
}

/// Jones::flagRight then flagLeft propagates an input flag through all four
/// correlations of a general linear-feed matrix, even for a zero sine term.
pub(super) fn propagate_flags(correlations: &[i32], flags: &mut ArrayD<bool>) -> usize {
    if correlations.len() != 4 || !correlations.iter().all(|code| matches!(code, 9..=12)) {
        return 0;
    }
    let mut added = 0;
    for mut channel in flags.axis_iter_mut(ndarray::Axis(1)) {
        if channel.iter().any(|flag| *flag) {
            for flag in &mut channel {
                if !*flag {
                    added += 1;
                }
                *flag = true;
            }
        }
    }
    added
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quarter_turns_rotate_rows_and_columns_with_the_jones_inverse_signs() {
        let original = [
            Complex32::new(1., 2.),
            Complex32::new(3., 4.),
            Complex32::new(5., 6.),
            Complex32::new(7., 8.),
        ];
        let quarter = std::f64::consts::FRAC_PI_2;
        for (first, second, expected) in [
            (
                quarter,
                0.,
                [-original[2], -original[3], original[0], original[1]],
            ),
            (
                0.,
                quarter,
                [-original[1], original[0], -original[3], original[2]],
            ),
            (
                quarter,
                quarter,
                [original[3], -original[2], -original[1], original[0]],
            ),
        ] {
            let mut data = ArrayD::from_shape_vec(IxDyn(&[4, 1]), original.to_vec()).unwrap();
            apply(0, &[9, 10, 11, 12], first, second, &mut data).unwrap();
            for (value, expected) in data.iter().zip(expected) {
                assert!((*value - expected).norm() < 1e-6);
            }
        }
    }

    #[test]
    fn incomplete_linear_correlations_fail_before_changing_the_buffer() {
        let mut data = ArrayD::from_elem(IxDyn(&[2, 2]), Complex32::new(1., 2.));
        let before = data.clone();
        assert!(matches!(
            apply(7, &[9, 12], 0.1, 0.2, &mut data),
            Err(ApplyExecutionError::UnsupportedParallacticAngleBasis {
                data_desc_id: 7,
                ..
            })
        ));
        assert_eq!(data, before);
    }
    #[test]
    fn linear_flags_spread_across_the_vector_but_preserve_other_channels() {
        let mut flags = ArrayD::from_elem(IxDyn(&[4, 3]), false);
        flags[[1, 0]] = true;
        flags[[2, 1]] = true;
        assert_eq!(propagate_flags(&[9, 10, 11, 12], &mut flags), 6);
        for corr in 0..4 {
            assert!(flags[[corr, 0]]);
            assert!(flags[[corr, 1]]);
            assert!(!flags[[corr, 2]]);
        }
        assert_eq!(propagate_flags(&[9, 10, 11, 12], &mut flags), 0);
    }
}
