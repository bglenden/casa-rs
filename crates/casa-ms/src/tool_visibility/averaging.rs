// SPDX-License-Identifier: LGPL-3.0-or-later
//! Flag-aware row-axis averaging for the public MeasurementSet tool.

use std::ops::{AddAssign, Div, Mul};

use casa_types::ArrayValue;
use ndarray::ArrayD;
use num_complex::{Complex32, Complex64};

use super::{input, shaped, vector};
use crate::MsResult;

pub(super) struct RowAverage {
    flags: ArrayD<bool>,
    weights: ArrayD<f32>,
    total_weight: Vec<f64>,
    result_flags: ArrayD<bool>,
}

impl RowAverage {
    pub(super) fn new(flags: ArrayValue, weights: ArrayValue) -> MsResult<Self> {
        let (ArrayValue::Bool(flags), ArrayValue::Float32(weights)) = (flags, weights) else {
            return Err(input(
                "visibility averaging requires Bool FLAG and Float WEIGHT",
            ));
        };
        if flags.shape() == [0] && weights.shape() == [0] {
            return Ok(Self {
                flags,
                weights,
                total_weight: Vec::new(),
                result_flags: shaped(&[0, 0, 1], Vec::new())?,
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
        let mut total_weight = vec![0.0; correlations * channels];
        let mut result_flags = vec![true; total_weight.len()];
        for (row_flags, row_weights) in memory_order(&flags)?
            .chunks_exact(total_weight.len())
            .zip(memory_order(&weights)?.chunks_exact(correlations))
        {
            for ((flagged, total), (&flag, &weight)) in result_flags
                .iter_mut()
                .zip(&mut total_weight)
                .zip(row_flags.iter().zip(row_weights.iter().cycle()))
            {
                if !flag {
                    *flagged = false;
                    *total += f64::from(weight);
                }
            }
        }
        Ok(Self {
            flags,
            weights,
            total_weight,
            result_flags: shaped(&[correlations, channels, 1], result_flags)?,
        })
    }

    pub(super) fn reduce(&self, data: &ArrayValue) -> MsResult<ArrayValue> {
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
        &self,
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
            return shaped(&[0, 0], Vec::new());
        }
        let correlations = data.shape()[0];
        let plane = self.total_weight.len();
        let mut sums = vec![Sum::default(); plane];
        for ((samples, flags), weights) in memory_order(data)?
            .chunks_exact(plane)
            .zip(memory_order(&self.flags)?.chunks_exact(plane))
            .zip(memory_order(&self.weights)?.chunks_exact(correlations))
        {
            for (sum, ((&sample, &flagged), &weight)) in sums
                .iter_mut()
                .zip(samples.iter().zip(flags).zip(weights.iter().cycle()))
            {
                if !flagged {
                    *sum += widen(sample) * f64::from(weight);
                }
            }
        }
        let means = sums
            .into_iter()
            .zip(&self.total_weight)
            .map(|(sum, &weight)| narrow(if weight > 0.0 { sum / weight } else { sum }))
            .collect();
        shaped(&data.shape()[..2], means)
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

pub(super) fn sigma(values: ArrayValue) -> MsResult<ArrayValue> {
    let ArrayValue::Float32(values) = values else {
        return Err(input("SIGMA must contain Float cells"));
    };
    if values.is_empty() {
        return Ok(ArrayValue::Float32(values));
    }
    let &[correlations, _] = values.shape() else {
        return Err(input("SIGMA requires a correlation and row axis"));
    };
    let mut squares = vec![0.0; correlations];
    for row in memory_order(&values)?.chunks_exact(correlations) {
        for (sum, &value) in squares.iter_mut().zip(row) {
            *sum += value * value;
        }
    }
    Ok(ArrayValue::Float32(vector(
        squares.into_iter().map(f32::sqrt).collect(),
    )))
}

fn memory_order<Value>(array: &ArrayD<Value>) -> MsResult<&[Value]> {
    array
        .as_slice_memory_order()
        .ok_or_else(|| input("visibility averaging requires contiguous arrays"))
}
