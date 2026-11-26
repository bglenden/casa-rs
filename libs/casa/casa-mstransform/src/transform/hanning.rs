// SPDX-License-Identifier: LGPL-3.0-or-later
//! Hanning smoothing on the selected and averaged frequency axis of an MS.

use std::ops::{Add, Mul};

use casa_core::ArrayValue;
use casa_tables::TableError;
use ndarray::{Array1, Array2, ArrayD, Axis};

use casa_ms::schema::main_table::OptionalMainColumn::{CorrectedData, Data, ModelData};

use super::channel_average::{
    AveragedStatistics, ChannelAverages, InputStatistic, SpectrumCells, sigma_from_weight,
    weight_from_sigma,
};
use super::visibility_columns::VisibilityColumns;

const COEFFICIENTS: [f32; 3] = [0.25, 0.5, 0.25];

pub(super) fn smooth(value: &mut ArrayValue, flag_shape: &[usize]) -> Result<(), TableError> {
    smooth_visibility(value, flag_shape, false)
}

pub(super) fn smooth_tool_array(
    value: &mut ArrayValue,
    flag_shape: &[usize],
) -> Result<(), TableError> {
    smooth_visibility(value, flag_shape, true)
}

fn smooth_visibility(
    value: &mut ArrayValue,
    flag_shape: &[usize],
    average_edges: bool,
) -> Result<(), TableError> {
    let ranks_match = flag_shape.len() == 2 || (average_edges && flag_shape.len() == 3);
    if !ranks_match || value.shape() != flag_shape {
        return Err(TableError::Schema(
            "visibility and FLAG must have matching [corr, chan] shapes for Hanning smoothing"
                .into(),
        ));
    }
    match value {
        ArrayValue::Complex32(array) => smooth_values(array, average_edges),
        ArrayValue::Float32(array) => smooth_values(array, average_edges),
        _ => {
            return Err(TableError::Schema(
                "visibility arrays must hold Complex32 or Float32".into(),
            ));
        }
    }
    Ok(())
}

fn smooth_values<T>(array: &mut ArrayD<T>, average_edges: bool)
where
    T: Copy + Add<Output = T> + Mul<f32, Output = T>,
{
    apply_interior(
        array,
        |[left, center, right]| {
            left * COEFFICIENTS[0] + center * COEFFICIENTS[1] + right * COEFFICIENTS[2]
        },
        average_edges.then_some(|[left, right]| left * 0.5 + right * 0.5),
    );
}

pub(super) fn smooth_regrid(value: &mut ArrayValue, flags: &ArrayValue) -> Result<(), TableError> {
    let ArrayValue::Bool(flags) = flags else {
        return Err(TableError::Schema("Hanning FLAG must be Bool".into()));
    };
    if value.shape() != flags.shape() || flags.ndim() != 2 {
        return Err(TableError::Schema(
            "visibility and FLAG must have matching [corr, chan] shapes for Hanning smoothing"
                .into(),
        ));
    }
    match value {
        ArrayValue::Complex32(array) => smooth_flagged_values(array, flags),
        ArrayValue::Float32(array) => smooth_flagged_values(array, flags),
        _ => {
            return Err(TableError::Schema(
                "Hanning visibility must be Complex32 or Float32".into(),
            ));
        }
    }
    Ok(())
}

fn smooth_flagged_values<T>(values: &mut ArrayD<T>, flags: &ArrayD<bool>)
where
    T: Copy + Add<Output = T> + Mul<f32, Output = T>,
{
    let channels = values.len_of(Axis(1));
    if channels < 2 {
        return;
    }
    let last = channels - 1;
    for (mut row, flags) in values
        .lanes_mut(Axis(1))
        .into_iter()
        .zip(flags.lanes(Axis(1)))
    {
        // SubMS uses Smooth::hanning with relaxed=True and TrueIsGood=False.
        // Both endpoints remain flagged. Their stored values use the edge masks.
        let first_value = if !flags[0] && !flags[1] {
            row[0] * (2.0 / 3.0) + row[1] * (1.0 / 3.0)
        } else {
            row[0]
        };
        let last_value = if flags[last] {
            row[last]
        } else if flags[last - 1] {
            row[last - 1]
        } else {
            row[last] * (2.0 / 3.0) + row[last - 1] * (1.0 / 3.0)
        };
        let mut left = row[0];
        for channel in 1..last {
            let centre = row[channel];
            let right = row[channel + 1];
            if !flags[channel] {
                let weights = match (flags[channel - 1], flags[channel + 1]) {
                    (false, false) => COEFFICIENTS,
                    (false, true) => [1.0 / 3.0, 2.0 / 3.0, 0.0],
                    (true, false) => [0.0, 2.0 / 3.0, 1.0 / 3.0],
                    (true, true) => [0.0, 1.0, 0.0],
                };
                row[channel] = left * weights[0] + centre * weights[1] + right * weights[2];
            }
            left = centre;
        }
        row[0] = first_value;
        row[last] = last_value;
    }
}

pub(super) fn flag(value: &mut ArrayValue) {
    let ArrayValue::Bool(flags) = value else {
        unreachable!("FLAG has already been validated")
    };
    apply_interior(flags, |[left, center, right]| left || center || right, None);
    let channels = flags.len_of(Axis(1));
    if channels > 0 {
        flags.index_axis_mut(Axis(1), 0).fill(true);
        flags.index_axis_mut(Axis(1), channels - 1).fill(true);
    }
}

fn apply_interior<T: Copy>(
    array: &mut ArrayD<T>,
    filter: impl Fn([T; 3]) -> T,
    edges: Option<fn([T; 2]) -> T>,
) {
    let channels = array.len_of(Axis(1));
    if channels < 3 {
        return;
    }
    for mut row in array.lanes_mut(Axis(1)) {
        let endpoints = edges.map(|edge| {
            [
                edge([row[0], row[1]]),
                edge([row[channels - 2], row[channels - 1]]),
            ]
        });
        let mut left = row[0];
        let mut center = row[1];
        for channel in 1..channels - 1 {
            let right = row[channel + 1];
            row[channel] = filter([left, center, right]);
            left = center;
            center = right;
        }
        if let Some([first, last]) = endpoints {
            row[0] = first;
            row[channels - 1] = last;
        }
    }
}

pub(super) fn tool_weight(
    value: &mut ArrayValue,
    flag_shape: &[usize],
    spectrum: bool,
) -> Result<(), TableError> {
    let ArrayValue::Float32(weights) = value else {
        return Err(TableError::Schema("Hanning weights must be Float32".into()));
    };
    if spectrum {
        if weights.shape() != flag_shape || !matches!(flag_shape.len(), 2 | 3) {
            return Err(TableError::Schema("WEIGHT_SPECTRUM must match FLAG".into()));
        }
        apply_interior(
            weights,
            |values| {
                if values.contains(&0.0) {
                    return 0.0;
                }
                let [left, center, right] = values.map(f64::from);
                (1.0 / (1.0 / (16.0 * left) + 1.0 / (4.0 * center) + 1.0 / (16.0 * right))) as f32
            },
            Some(|_| 0.0),
        );
    } else {
        if flag_shape.len() != 2 || weights.shape() != &flag_shape[..1] {
            return Err(TableError::Schema(
                "WEIGHT must have one value per correlation".into(),
            ));
        }
        weights.map_inplace(|weight| *weight = (f64::from(*weight) * 8.0 / 3.0) as f32);
    }
    Ok(())
}

pub(super) fn statistics(
    (scalar_weight, scalar_sigma): &(ArrayValue, ArrayValue),
    spectra: SpectrumCells<'_>,
    flags: &ArrayValue,
    bins: &[Vec<usize>],
    averages: Option<&ChannelAverages<'_>>,
    visibility: &VisibilityColumns,
    time_average: bool,
) -> Result<AveragedStatistics, TableError> {
    let (weight, observed_weight, flags) = if let Some(averages) = averages {
        let (weight, observed_weight) = averages.weight_spectra();
        (weight, observed_weight, averages.bin_flags())
    } else {
        let ArrayValue::Bool(flags) = flags else {
            return Err(TableError::Schema("FLAG must hold Bool".into()));
        };
        let &[correlations, channels] = flags.shape() else {
            return Err(TableError::Schema(
                "FLAG must be rank-2 [corr, chan]".into(),
            ));
        };
        let shape = [correlations, channels];
        let weight = InputStatistic::from_cells(scalar_weight, spectra.weight, shape, "WEIGHT")?;
        let sigma = InputStatistic::from_cells(scalar_sigma, spectra.sigma, shape, "SIGMA")?;
        let has = |source| visibility.copies().iter().any(|copy| copy.source == source);
        let dims = (correlations, bins.len());
        let calibrated =
            || Array2::from_shape_fn(dims, |(corr, bin)| weight.value(corr, bins[bin][0]));
        let observed = || {
            Array2::from_shape_fn(dims, |(corr, bin)| {
                weight_from_sigma(sigma.value(corr, bins[bin][0]))
            })
        };
        let (weight, observed_weight) = if has(Data) && has(CorrectedData) {
            (calibrated(), observed())
        } else {
            let weight = if time_average || has(CorrectedData) {
                calibrated()
            } else if has(Data) {
                observed()
            } else if has(ModelData) {
                Array2::ones(dims)
            } else {
                // Keep defined noise columns for FLOAT_DATA and LAG_DATA.
                calibrated()
            };
            (weight.clone(), weight)
        };
        let selected = Array2::from_shape_fn(dims, |(corr, bin)| flags[[corr, bins[bin][0]]]);
        (weight, observed_weight, selected)
    };
    let (weight, weight_spectrum) = filter_noise(weight, &flags, |weight| weight);
    let (sigma, sigma_spectrum) = filter_noise(observed_weight, &flags, sigma_from_weight);
    Ok(AveragedStatistics {
        weight: ArrayValue::Float32(weight.into_dyn()),
        sigma: ArrayValue::Float32(sigma.into_dyn()),
        weight_spectrum: ArrayValue::Float32(weight_spectrum.into_dyn()),
        sigma_spectrum: ArrayValue::Float32(sigma_spectrum.into_dyn()),
    })
}

fn filter_noise(
    mut weights: Array2<f32>,
    flags: &Array2<bool>,
    convert: impl Fn(f32) -> f32,
) -> (Array1<f32>, Array2<f32>) {
    let channels = weights.ncols();
    let mut scalars = Array1::zeros(weights.nrows());
    if channels == 0 {
        return (scalars, weights);
    }
    for ((mut row, mask), scalar) in weights
        .rows_mut()
        .into_iter()
        .zip(flags.rows())
        .zip(&mut scalars)
    {
        let mut sum = 0.;
        let mut unflagged_sum = 0.;
        let mut unflagged = 0;
        let mut left = 0.;
        let mut center = row[0];
        for channel in 0..channels {
            let mut output = center;
            if channel > 0 && channel + 1 < channels {
                let right = row[channel + 1];
                let inputs = [left, center, right];
                let mut variance = 0.;
                let mut flagged = false;
                for (offset, (weight, coefficient)) in
                    inputs.into_iter().zip(COEFFICIENTS).enumerate()
                {
                    if weight <= f32::MIN_POSITIVE {
                        flagged = true;
                    } else {
                        // An unknown (NaN) variance must propagate through
                        // the filter. Zero or negative weight marks a flag.
                        variance += coefficient * coefficient / weight;
                    }
                    flagged |= mask[channel - 1 + offset];
                }
                output = if variance > f32::MIN_POSITIVE {
                    variance.recip()
                } else {
                    variance
                };
                if !flagged {
                    unflagged_sum += convert(output);
                    unflagged += 1;
                }
            }
            row[channel] = convert(output);
            sum += row[channel];
            left = center;
            center = row.get(channel + 1).copied().unwrap_or_default();
        }
        *scalar = if unflagged == 0 {
            sum / channels as f32
        } else {
            unflagged_sum / unflagged as f32
        };
    }
    (scalars, weights)
}
