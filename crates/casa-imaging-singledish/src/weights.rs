// SPDX-License-Identifier: LGPL-3.0-or-later
//! The weights the gridder gives each spectrum (`SDGrid::pickWeights`), and
//! the weight below which `tsdimaging` masks its image
//! (`do_weight_mask`).

use ndarray::{Array2, ArrayView2, ArrayView3};

use crate::SingleDishError;

/// `SDGrid::pickWeights` from `WEIGHT`, `[spectrum][correlation]`: each
/// spectrum's weight, the same in each of its `channels` channels
/// (`[spectrum][channel]`). One correlation's weight is taken as it is; two
/// correlations' `w₀` and `w₁` combine as the weight of their mean,
/// `4 w₀ w₁ / (w₀ + w₁)`, and as 0 when their sum is zero (CAS-9957).
///
/// # Errors
///
/// Weights of more than two correlations: CASA refuses to image full-Stokes
/// data.
pub fn weights_from_rows(
    weight: ArrayView2<'_, f32>,
    channels: usize,
) -> Result<Array2<f32>, SingleDishError> {
    let (spectra, correlations) = weight.dim();
    check_correlations(correlations)?;
    let combine = |spectrum: usize| match correlations {
        1 => weight[[spectrum, 0]],
        _ => combined(weight[[spectrum, 0]], weight[[spectrum, 1]]),
    };
    let mut weights = Array2::zeros((spectra, channels));
    for (spectrum, mut row) in weights.rows_mut().into_iter().enumerate() {
        row.fill(combine(spectrum));
    }
    Ok(weights)
}

/// `SDGrid::pickWeights` from `WEIGHT_SPECTRUM`,
/// `[spectrum][channel][correlation]`: [`weights_from_rows`] for each
/// channel.
///
/// # Errors
///
/// Weights of more than two correlations.
pub fn weights_from_spectra(weight: ArrayView3<'_, f32>) -> Result<Array2<f32>, SingleDishError> {
    let (spectra, channels, correlations) = weight.dim();
    check_correlations(correlations)?;
    Ok(Array2::from_shape_fn(
        (spectra, channels),
        |(spectrum, channel)| match correlations {
            1 => weight[[spectrum, channel, 0]],
            _ => combined(
                weight[[spectrum, channel, 0]],
                weight[[spectrum, channel, 1]],
            ),
        },
    ))
}

fn check_correlations(correlations: usize) -> Result<(), SingleDishError> {
    if correlations == 1 || correlations == 2 {
        Ok(())
    } else {
        Err(SingleDishError::Correlations(format!(
            "imaging single-dish data of {correlations} correlations is not supported; CASA \
             images one or two (full-Stokes data, four correlations, are refused)"
        )))
    }
}

/// Two correlations' weights combined (`toStokesWeight`).
fn combined(w0: f32, w1: f32) -> f32 {
    let denominator = w0 + w1;
    if denominator.abs() < f32::MIN_POSITIVE {
        0.0
    } else {
        4.0 * (w0 * w1) / denominator
    }
}

/// `do_weight_mask`: the weight a pixel must exceed to stay unmasked,
/// `minweight` times the median of the positive weights through every
/// plane (`ia.statistics` of the weight image where it is positive), or
/// `None` when no weight is positive (CASA then sets no mask). The median
/// of an even count is the mean of the middle two.
///
/// One departure (`docs/CASA (C++) bugs.md`): CASA writes the threshold into
/// its mask expression with `%f`, six decimal places, so a threshold under
/// 5e-7 masks nothing. This is the threshold itself.
#[must_use]
pub fn weight_mask_threshold(weights: &[f32], minweight: f64) -> Option<f64> {
    let mut positive: Vec<f32> = weights.iter().copied().filter(|&w| w > 0.0).collect();
    if positive.is_empty() {
        return None;
    }
    let even = positive.len().is_multiple_of(2);
    let middle = positive.len() / 2;
    let (below, &mut upper, _) = positive.select_nth_unstable_by(middle, f32::total_cmp);
    let median = if even {
        // An even count: the lower middle is the largest below.
        let lower = below.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        (f64::from(lower) + f64::from(upper)) / 2.0
    } else {
        f64::from(upper)
    };
    Some(median * minweight)
}

#[cfg(test)]
mod tests {
    use ndarray::{Array3, array};

    use super::*;

    #[test]
    fn two_correlations_combine_as_the_weight_of_their_mean() {
        let rows = weights_from_rows(array![[2.0_f32, 2.0], [1.0, 3.0], [0.0, 0.0]].view(), 3)
            .expect("weights");
        assert_eq!(rows.row(0).to_vec(), vec![4.0; 3]);
        assert_eq!(rows.row(1).to_vec(), vec![3.0; 3]);
        assert_eq!(rows.row(2).to_vec(), vec![0.0; 3]);
        let one = weights_from_rows(array![[5.0_f32]].view(), 2).expect("weights");
        assert_eq!(one.row(0).to_vec(), vec![5.0; 2]);
    }

    #[test]
    fn spectra_combine_channel_by_channel() {
        let mut spectrum = Array3::zeros((1, 2, 2));
        spectrum[[0, 0, 0]] = 1.0_f32;
        spectrum[[0, 0, 1]] = 1.0;
        spectrum[[0, 1, 0]] = 2.0;
        spectrum[[0, 1, 1]] = 6.0;
        let weights = weights_from_spectra(spectrum.view()).expect("weights");
        assert_eq!(weights.row(0).to_vec(), vec![2.0, 6.0]);
    }

    #[test]
    fn four_correlations_are_refused() {
        assert!(weights_from_rows(Array2::<f32>::ones((1, 4)).view(), 1).is_err());
        assert!(weights_from_spectra(Array3::<f32>::ones((1, 1, 4)).view()).is_err());
    }

    #[test]
    fn the_mask_threshold_is_minweight_times_the_median_positive_weight() {
        assert_eq!(
            weight_mask_threshold(&[0.0, 3.0, 1.0, 2.0, -1.0], 0.5),
            Some(1.0)
        );
        assert_eq!(
            weight_mask_threshold(&[4.0, 1.0, 0.0, 3.0, 2.0], 0.5),
            Some(1.25)
        );
        assert_eq!(weight_mask_threshold(&[0.0, 0.0], 0.5), None);
    }
}
