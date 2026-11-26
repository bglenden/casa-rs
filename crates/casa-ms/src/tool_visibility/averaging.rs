// SPDX-License-Identifier: LGPL-3.0-or-later
//! Flag-aware row and integration averaging for the public MeasurementSet tool.

use std::ops::{AddAssign, Div, Mul};

use casa_core::ArrayValue;
use ndarray::ArrayD;
use num_complex::{Complex32, Complex64};

use super::baseline_axis::{element_count, filled};
use super::{BaselineGrid, input, shaped};
use crate::MsResult;

pub(super) struct RowAverage<'a> {
    grid: Option<&'a BaselineGrid>,
    flags: ArrayD<bool>,
    weights: ArrayD<f32>,
    total_weight: Vec<f64>,
    result_flags: ArrayD<bool>,
}

impl<'a> RowAverage<'a> {
    pub(super) fn new(
        flags: ArrayValue,
        weights: ArrayValue,
        grid: Option<&'a BaselineGrid>,
    ) -> MsResult<Self> {
        let (ArrayValue::Bool(flags), ArrayValue::Float32(weights)) = (flags, weights) else {
            return Err(input(
                "visibility averaging requires Bool FLAG and Float WEIGHT",
            ));
        };
        let groups = grid.map_or(1, BaselineGrid::baseline_count);
        if flags.shape() == [0] && weights.shape() == [0] {
            return Ok(Self {
                grid,
                flags,
                weights,
                total_weight: Vec::new(),
                result_flags: shaped(&[0, 0, groups], Vec::new())?,
            });
        }
        let &[correlations, channels, rows] = flags.shape() else {
            return Err(input(
                "visibility averaging requires correlation, channel and row axes",
            ));
        };
        if correlations == 0 || channels == 0 || weights.shape() != [correlations, rows] {
            return Err(input("WEIGHT does not match the visibility averaging axes"));
        }
        let shape = [correlations, channels, groups];
        Ok(Self {
            grid,
            flags,
            weights,
            total_weight: Vec::new(),
            result_flags: shaped(&shape, filled(element_count(&shape)?, true)?)?,
        })
    }

    pub(super) fn reduce(&mut self, data: &ArrayValue) -> MsResult<ArrayValue> {
        match data {
            ArrayValue::Float32(values) => Ok(ArrayValue::Float32(self.mean(
                values,
                f64::from,
                |value| value as f32,
            )?)),
            ArrayValue::Complex32(values) => Ok(ArrayValue::Complex32(self.mean(
                values,
                |value| Complex64::new(f64::from(value.re), f64::from(value.im)),
                |value| Complex32::new(value.re as f32, value.im as f32),
            )?)),
            _ => Err(input(
                "visibility averaging requires Float or Complex cells",
            )),
        }
    }

    fn mean<Input, Sum>(
        &mut self,
        data: &ArrayD<Input>,
        widen: impl Fn(Input) -> Sum,
        narrow: impl Fn(Sum) -> Input,
    ) -> MsResult<ArrayD<Input>>
    where
        Input: Copy,
        Sum: Default + Clone + AddAssign + Mul<f64, Output = Sum> + Div<f64, Output = Sum>,
    {
        if data.shape() != self.flags.shape() {
            return Err(input("FLAG and visibility averaging axes differ"));
        }
        if data.is_empty() {
            let shape = if self.grid.is_some() {
                self.result_flags.shape()
            } else {
                &[0, 0]
            };
            return shaped(shape, Vec::new());
        }
        let correlations = data.shape()[0];
        let length = self.result_flags.len();
        let mut sums = filled(length, Sum::default())?;
        let samples = memory_order(data)?;
        if self.total_weight.is_empty() {
            self.total_weight = filled(length, 0.0)?;
            self.accumulate::<_, _, true>(samples, &mut sums, correlations, widen)?;
        } else {
            self.accumulate::<_, _, false>(samples, &mut sums, correlations, widen)?;
        }
        let means = sums
            .into_iter()
            .zip(&self.total_weight)
            .map(|(sum, &weight)| narrow(if weight > 0.0 { sum / weight } else { sum }))
            .collect();
        let shape = if self.grid.is_some() {
            self.result_flags.shape()
        } else {
            &data.shape()[..2]
        };
        shaped(shape, means)
    }

    /// Compute the shared weight sum and flags with the first product.
    fn accumulate<Input, Sum, const CAPTURE_WEIGHTS: bool>(
        &mut self,
        samples: &[Input],
        sums: &mut [Sum],
        correlations: usize,
        widen: impl Fn(Input) -> Sum,
    ) -> MsResult<()>
    where
        Input: Copy,
        Sum: AddAssign + Mul<f64, Output = Sum>,
    {
        let plane = correlations * self.flags.shape()[1];
        let flags = memory_order(&self.flags)?;
        let weights = memory_order(&self.weights)?;
        let result_flags = self
            .result_flags
            .as_slice_memory_order_mut()
            .ok_or_else(|| input("averaged flags are not contiguous"))?;
        let mut add_row = |dest: usize, row: &[Input], row_flags: &[bool], row_weights: &[f32]| {
            // One WEIGHT vector covers every channel in this row.
            for (((sums, totals), result_flags), (samples, flags)) in sums[dest..dest + plane]
                .chunks_exact_mut(correlations)
                .zip(self.total_weight[dest..dest + plane].chunks_exact_mut(correlations))
                .zip(result_flags[dest..dest + plane].chunks_exact_mut(correlations))
                .zip(
                    row.chunks_exact(correlations)
                        .zip(row_flags.chunks_exact(correlations)),
                )
            {
                for (((sum, total), result_flag), ((&sample, &flagged), &weight)) in sums
                    .iter_mut()
                    .zip(totals)
                    .zip(result_flags)
                    .zip(samples.iter().zip(flags).zip(row_weights))
                {
                    if !flagged {
                        let weight = f64::from(weight);
                        *sum += widen(sample) * weight;
                        if CAPTURE_WEIGHTS {
                            *total += weight;
                            *result_flag = false;
                        }
                    }
                }
            }
        };
        if let Some(grid) = self.grid {
            for (baseline, source) in grid.reduction_rows() {
                let start = source * plane;
                let weight = source * correlations;
                add_row(
                    baseline * plane,
                    &samples[start..start + plane],
                    &flags[start..start + plane],
                    &weights[weight..weight + correlations],
                );
            }
        } else {
            for ((row, row_flags), row_weights) in samples
                .chunks_exact(plane)
                .zip(flags.chunks_exact(plane))
                .zip(weights.chunks_exact(correlations))
            {
                add_row(0, row, row_flags, row_weights);
            }
        }
        Ok(())
    }

    pub(super) fn into_fields(self) -> (ArrayD<bool>, ArrayD<bool>, ArrayValue) {
        (
            self.flags,
            self.result_flags,
            ArrayValue::Float32(self.weights),
        )
    }
}

pub(super) fn common_id(ids: &mut Vec<i32>) {
    if let Some(&first) = ids.first() {
        let common = if ids.iter().all(|&id| id == first) {
            first
        } else {
            -1
        };
        ids.truncate(1);
        ids[0] = common;
    }
}

pub(super) fn sigma(values: ArrayValue, grid: Option<&BaselineGrid>) -> MsResult<ArrayValue> {
    sum_row_values(values, grid, true)
}

pub(super) fn sum_weights(values: ArrayValue, grid: Option<&BaselineGrid>) -> MsResult<ArrayValue> {
    sum_row_values(values, grid, false)
}

fn sum_row_values(
    values: ArrayValue,
    grid: Option<&BaselineGrid>,
    sigma: bool,
) -> MsResult<ArrayValue> {
    let ArrayValue::Float32(values) = values else {
        return Err(input("WEIGHT and SIGMA must contain Float cells"));
    };
    if values.is_empty() {
        return Ok(ArrayValue::Float32(values));
    }
    let &[correlations, _] = values.shape() else {
        return Err(input("WEIGHT and SIGMA require correlation and row axes"));
    };
    let mut shape = vec![correlations];
    if let Some(grid) = grid {
        shape.push(grid.baseline_count());
    }
    let mut squares = filled(element_count(&shape)?, 0.0)?;
    let samples = memory_order(&values)?;
    let mut add_row = |dest: usize, row: &[f32]| {
        for (sum, &value) in squares[dest..dest + correlations].iter_mut().zip(row) {
            *sum += if sigma { value * value } else { value };
        }
    };
    if let Some(grid) = grid {
        for (baseline, source) in grid.reduction_rows() {
            let start = source * correlations;
            add_row(
                baseline * correlations,
                &samples[start..start + correlations],
            );
        }
    } else {
        for row in samples.chunks_exact(correlations) {
            add_row(0, row);
        }
    }
    if sigma {
        for value in &mut squares {
            *value = value.sqrt();
        }
    }
    Ok(ArrayValue::Float32(shaped(&shape, squares)?))
}

pub(super) fn row_flag_sums(flags: &ArrayD<bool>, empty: bool) -> MsResult<ArrayD<i64>> {
    let (shape, rows) = match flags.shape() {
        [correlations, channels, rows] => ([*correlations, *channels], *rows),
        _ if empty => ([0, 0], 0),
        _ => return Err(input("FLAG column has no row axis")),
    };
    let samples = shape.iter().product();
    let mut sums = vec![0_i64; samples];
    let memory = flags
        .as_slice_memory_order()
        .ok_or_else(|| input("FLAG block is not contiguous"))?;
    for row in memory.chunks_exact(samples.max(1)).take(rows) {
        for (sum, &flagged) in sums.iter_mut().zip(row) {
            *sum += i64::from(flagged);
        }
    }
    shaped(&shape, sums)
}

fn memory_order<Value>(array: &ArrayD<Value>) -> MsResult<&[Value]> {
    array
        .as_slice_memory_order()
        .ok_or_else(|| input("visibility averaging requires contiguous arrays"))
}
