// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility-domain algebra for bounded sparse-model refit experiments.
//!
//! Unlike an image-residual norm, this objective is the weighted
//! measurement-space least-squares objective.  It is therefore suitable for
//! judging coefficient changes even when the normal-equation image has
//! spatially correlated noise.

use num_complex::Complex32;

use crate::ImagingError;

#[derive(Debug, Default)]
struct CompensatedSum {
    sum: f64,
    correction: f64,
}

impl CompensatedSum {
    fn add(&mut self, value: f64) {
        let next = self.sum + value;
        if self.sum.abs() >= value.abs() {
            self.correction += (self.sum - next) + value;
        } else {
            self.correction += (value - next) + self.sum;
        }
        self.sum = next;
    }

    fn total(self) -> f64 {
        self.sum + self.correction
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct WeightedVisibilityObjective {
    pub(crate) samples: usize,
    pub(crate) parallel_hands: usize,
    pub(crate) weight_sum_per_hand: f64,
    pub(crate) weighted_residual_power: f64,
    pub(crate) weighted_rms_per_hand: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct WeightedVisibilityFit {
    pub(crate) samples: usize,
    pub(crate) parallel_hands: usize,
    pub(crate) weight_sum_per_hand: f64,
    pub(crate) dirty_power: f64,
    pub(crate) residual_power: f64,
    pub(crate) prediction_power: f64,
    pub(crate) data_prediction_cross_re: f64,
    pub(crate) data_prediction_cross_im: f64,
    pub(crate) real_gain_minimizer: f64,
    pub(crate) complex_gain_minimizer_re: f64,
    pub(crate) complex_gain_minimizer_im: f64,
    pub(crate) quadratic_closure_error: f64,
}

fn invalid(message: impl Into<String>) -> ImagingError {
    ImagingError::InvalidRequest(message.into())
}

fn complex_power(value: Complex32) -> f64 {
    let re = f64::from(value.re);
    let im = f64::from(value.im);
    re.mul_add(re, im * im)
}

#[cfg(test)]
pub(crate) fn weighted_parallel_hand_objective<I>(
    samples: I,
) -> Result<WeightedVisibilityObjective, ImagingError>
where
    I: IntoIterator<Item = (f32, [Complex32; 2])>,
{
    let mut sample_count = 0usize;
    let mut weight_sum = CompensatedSum::default();
    let mut weighted_power = CompensatedSum::default();
    for (weight, residuals) in samples {
        if !(weight.is_finite() && weight > 0.0) {
            return Err(invalid(
                "visibility-domain refit requires finite positive sample weights",
            ));
        }
        if residuals
            .iter()
            .any(|value| !(value.re.is_finite() && value.im.is_finite()))
        {
            return Err(invalid(
                "visibility-domain refit residual contains a non-finite value",
            ));
        }
        let weight = f64::from(weight);
        let sample_power = residuals.into_iter().map(complex_power).sum::<f64>();
        weight_sum.add(weight);
        weighted_power.add(weight * sample_power);
        sample_count = sample_count.saturating_add(1);
    }
    let weight_sum = weight_sum.total();
    let weighted_power = weighted_power.total();
    if sample_count == 0 || !(weight_sum.is_finite() && weight_sum > 0.0) {
        return Err(invalid(
            "visibility-domain refit objective requires at least one weighted sample",
        ));
    }
    if !(weighted_power.is_finite() && weighted_power >= 0.0) {
        return Err(invalid(
            "visibility-domain refit objective produced invalid residual power",
        ));
    }
    let parallel_hands = 2usize;
    let weighted_rms_per_hand = (weighted_power / (weight_sum * parallel_hands as f64)).sqrt();
    Ok(WeightedVisibilityObjective {
        samples: sample_count,
        parallel_hands,
        weight_sum_per_hand: weight_sum,
        weighted_residual_power: weighted_power,
        weighted_rms_per_hand,
    })
}

pub(crate) fn weighted_parallel_hand_fit<I>(
    samples: I,
) -> Result<WeightedVisibilityFit, ImagingError>
where
    I: IntoIterator<Item = (f32, [Complex32; 2], [Complex32; 2])>,
{
    let mut sample_count = 0usize;
    let mut weight_sum = CompensatedSum::default();
    let mut dirty_power = CompensatedSum::default();
    let mut residual_power = CompensatedSum::default();
    let mut prediction_power = CompensatedSum::default();
    let mut cross_re = CompensatedSum::default();
    let mut cross_im = CompensatedSum::default();
    for (weight, observed, residual) in samples {
        if !(weight.is_finite() && weight > 0.0) {
            return Err(invalid(
                "visibility-domain refit requires finite positive sample weights",
            ));
        }
        if observed
            .iter()
            .chain(&residual)
            .any(|value| !(value.re.is_finite() && value.im.is_finite()))
        {
            return Err(invalid(
                "visibility-domain refit fit contains a non-finite complex value",
            ));
        }
        let weight = f64::from(weight);
        weight_sum.add(weight);
        for (datum, remainder) in observed.into_iter().zip(residual) {
            let prediction = datum - remainder;
            dirty_power.add(weight * complex_power(datum));
            residual_power.add(weight * complex_power(remainder));
            prediction_power.add(weight * complex_power(prediction));
            let prediction_re = f64::from(prediction.re);
            let prediction_im = f64::from(prediction.im);
            let datum_re = f64::from(datum.re);
            let datum_im = f64::from(datum.im);
            cross_re.add(weight * prediction_re.mul_add(datum_re, prediction_im * datum_im));
            cross_im.add(weight * prediction_re.mul_add(datum_im, -(prediction_im * datum_re)));
        }
        sample_count = sample_count.saturating_add(1);
    }
    let weight_sum = weight_sum.total();
    let dirty_power = dirty_power.total();
    let residual_power = residual_power.total();
    let prediction_power = prediction_power.total();
    let data_prediction_cross_re = cross_re.total();
    let data_prediction_cross_im = cross_im.total();
    if sample_count == 0 || !(weight_sum.is_finite() && weight_sum > 0.0) {
        return Err(invalid(
            "visibility-domain refit fit requires at least one weighted sample",
        ));
    }
    if ![
        dirty_power,
        residual_power,
        prediction_power,
        data_prediction_cross_re,
        data_prediction_cross_im,
    ]
    .into_iter()
    .all(f64::is_finite)
        || dirty_power < 0.0
        || residual_power < 0.0
        || prediction_power <= 0.0
    {
        return Err(invalid(
            "visibility-domain refit fit produced invalid sufficient statistics",
        ));
    }
    let real_gain_minimizer = data_prediction_cross_re / prediction_power;
    let complex_gain_minimizer_re = real_gain_minimizer;
    let complex_gain_minimizer_im = data_prediction_cross_im / prediction_power;
    let quadratic_closure_error =
        residual_power - (dirty_power - 2.0 * data_prediction_cross_re + prediction_power);
    Ok(WeightedVisibilityFit {
        samples: sample_count,
        parallel_hands: 2,
        weight_sum_per_hand: weight_sum,
        dirty_power,
        residual_power,
        prediction_power,
        data_prediction_cross_re,
        data_prediction_cross_im,
        real_gain_minimizer,
        complex_gain_minimizer_re,
        complex_gain_minimizer_im,
        quadratic_closure_error,
    })
}

#[cfg(test)]
mod tests {
    use num_complex::Complex32;

    use super::{weighted_parallel_hand_fit, weighted_parallel_hand_objective};

    #[test]
    fn weighted_parallel_hand_objective_uses_one_weight_for_each_hand() {
        let objective = weighted_parallel_hand_objective([
            (2.0, [Complex32::new(3.0, 4.0), Complex32::new(0.0, 1.0)]),
            (0.5, [Complex32::new(2.0, 0.0), Complex32::new(0.0, -2.0)]),
        ])
        .expect("weighted objective");

        assert_eq!(objective.samples, 2);
        assert_eq!(objective.parallel_hands, 2);
        assert_eq!(objective.weight_sum_per_hand, 2.5);
        assert_eq!(objective.weighted_residual_power, 56.0);
        assert_eq!(objective.weighted_rms_per_hand, (56.0f64 / 5.0).sqrt());
    }

    #[test]
    fn weighted_parallel_hand_objective_rejects_invalid_inputs() {
        assert!(
            weighted_parallel_hand_objective(std::iter::empty::<(f32, [Complex32; 2])>()).is_err()
        );
        assert!(
            weighted_parallel_hand_objective([(
                0.0,
                [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
            )])
            .is_err()
        );
        assert!(
            weighted_parallel_hand_objective([(
                1.0,
                [Complex32::new(f32::NAN, 0.0), Complex32::new(1.0, 0.0),],
            )])
            .is_err()
        );
    }

    #[test]
    fn weighted_parallel_hand_fit_recovers_the_applied_complex_gain() {
        let unit_prediction = [Complex32::new(1.0, 2.0), Complex32::new(-0.5, 0.25)];
        let gain = Complex32::new(2.0, -0.5);
        let observed = unit_prediction.map(|value| gain * value);
        let residual = std::array::from_fn(|index| observed[index] - unit_prediction[index]);
        let fit = weighted_parallel_hand_fit([(3.0, observed, residual)]).expect("weighted fit");

        assert_eq!(fit.samples, 1);
        assert!((fit.complex_gain_minimizer_re - 2.0).abs() < 1.0e-12);
        assert!((fit.complex_gain_minimizer_im + 0.5).abs() < 1.0e-12);
        assert!((fit.real_gain_minimizer - 2.0).abs() < 1.0e-12);
        assert!(fit.quadratic_closure_error.abs() < 1.0e-12);
    }
}
