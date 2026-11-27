// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sample and flag interpolation for MeasurementSet frequency regridding.

use std::ops::{Add, Div, Mul, Sub};

use casa_core::ArrayValue;
use casa_tables::TableError;
use ndarray::{Array2, ArrayView2, ArrayView3, Ix2, ShapeBuilder};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Method {
    Nearest,
    Linear,
    Cubic,
    Spline,
    FftShift,
}

impl Method {
    pub(super) fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "nearest" => Self::Nearest,
            "linear" => Self::Linear,
            "cubic" => Self::Cubic,
            "spline" => Self::Spline,
            "fftshift" => Self::FftShift,
            _ => return None,
        })
    }
}

struct Sample {
    // Indices in monotonic order; the actual MS frequency axis may descend.
    bracket: [usize; 2],
    fraction: f64,
    outside: bool,
    cubic: Option<[(usize, f32); 4]>,
}

pub(super) struct Interpolation {
    samples: Vec<Sample>,
    centres: Vec<f64>,
    descending: bool,
    method: Method,
}

impl Interpolation {
    pub(super) fn plan(input: &[f64], output: &[f64], method: Method) -> Result<Self, TableError> {
        let descending = input.first() > input.last();
        let mut centres = input.to_vec();
        if descending {
            centres.reverse();
        }
        if centres.is_empty() || centres.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(TableError::Schema(
                "Regridding requires ordered, distinct frequencies.".into(),
            ));
        }
        let n = centres.len();
        let last = n - 1;
        let method = if method == Method::Cubic && n < 4 {
            Method::Linear
        } else {
            method
        };
        let samples = output
            .iter()
            .map(|&frequency| {
                let include_equal = descending && matches!(method, Method::Cubic | Method::Spline);
                let at = centres.partition_point(|value| {
                    *value < frequency || (include_equal && *value == frequency)
                });
                if n == 1 {
                    return Sample {
                        bracket: [0, 0],
                        fraction: 0.,
                        outside: true,
                        cubic: None,
                    };
                }
                let right = at.clamp(1, last);
                let left = right - 1;
                let fraction = (frequency - centres[left]) / (centres[right] - centres[left]);
                let outside = frequency < centres[0] || frequency > centres[last];
                let (bracket, cubic) = match method {
                    Method::Nearest => {
                        let index = if fraction < 0.5 || (fraction == 0.5 && !descending) {
                            left
                        } else {
                            right
                        };
                        ([index; 2], None)
                    }
                    Method::Linear | Method::FftShift => {
                        let indices = if fraction <= f64::EPSILON {
                            [left; 2]
                        } else if fraction >= 1. - f64::EPSILON {
                            [right; 2]
                        } else {
                            [left, right]
                        };
                        (indices, None)
                    }
                    Method::Cubic => {
                        let start = left.saturating_sub(1).min(n - 4);
                        let coefficients = std::array::from_fn(|offset| {
                            let index = start + offset;
                            let x = centres[index];
                            let weight = (start..start + 4)
                                .filter(|&other| other != index)
                                .map(|other| (frequency - centres[other]) / (x - centres[other]))
                                .product::<f64>();
                            (index, weight as f32)
                        });
                        ([left, right], Some(coefficients))
                    }
                    Method::Spline => ([left, right], None),
                };
                Sample {
                    bracket,
                    fraction,
                    outside,
                    cubic,
                }
            })
            .collect();
        Ok(Self {
            samples,
            centres,
            descending,
            method,
        })
    }

    fn physical(&self, channel: usize) -> usize {
        if self.descending {
            self.centres.len() - 1 - channel
        } else {
            channel
        }
    }

    fn values<T: SampleValue>(&self, input: ArrayView2<'_, T>) -> Array2<T> {
        // MeasurementSet cells store all correlations of a channel together.
        let mut output = Array2::default((input.nrows(), self.samples.len()).f());
        let mut coefficients = Vec::new();
        let mut curvature = Vec::new();
        for (corr, mut row) in output.rows_mut().into_iter().enumerate() {
            let get = |channel| input[[corr, self.physical(channel)]];
            if self.method == Method::Spline {
                second_derivatives(
                    self.centres.len(),
                    |channel| self.centres[self.physical(channel)],
                    |channel| input[[corr, channel]],
                    &mut coefficients,
                    &mut curvature,
                );
            }
            for (output, sample) in row.iter_mut().zip(&self.samples) {
                let [left, right] = sample.bracket;
                *output = if let Some(cubic) = sample.cubic {
                    cubic
                        .into_iter()
                        .fold(T::default(), |sum, (channel, coefficient)| {
                            sum + get(channel) * coefficient
                        })
                } else if left == right {
                    get(left)
                } else if self.method == Method::Spline {
                    let distance = (self.centres[right] - self.centres[left]) as f32;
                    let b = sample.fraction as f32;
                    let a = 1. - b;
                    get(left) * a
                        + get(right) * b
                        + (curvature[self.physical(left)] * (a * a * a - a)
                            + curvature[self.physical(right)] * (b * b * b - b))
                            * (distance * distance / 6.)
                } else {
                    let a = get(left);
                    a + (get(right) - a) * sample.fraction as f32
                };
            }
        }
        output
    }

    pub(super) fn apply(&self, value: ArrayValue) -> Result<ArrayValue, TableError> {
        let error =
            || TableError::Schema("Regridding needs matching correlation and channel axes.".into());
        if value.shape().get(1) != Some(&self.centres.len()) {
            return Err(error());
        }
        match value {
            ArrayValue::Float32(data) => {
                let input = data
                    .view()
                    .into_dimensionality::<Ix2>()
                    .map_err(|_| error())?;
                Ok(ArrayValue::Float32(self.values(input).into_dyn()))
            }
            ArrayValue::Complex32(data) => {
                let input = data
                    .view()
                    .into_dimensionality::<Ix2>()
                    .map_err(|_| error())?;
                Ok(ArrayValue::Complex32(self.values(input).into_dyn()))
            }
            ArrayValue::Bool(data) => {
                let input = data
                    .view()
                    .into_dimensionality::<Ix2>()
                    .map_err(|_| error())?;
                let flags = Array2::from_shape_fn(
                    (input.nrows(), self.samples.len()).f(),
                    |(corr, chan)| {
                        let sample = &self.samples[chan];
                        let [left, right] = sample.bracket.map(|index| self.physical(index));
                        sample.outside || input[[corr, left]] || input[[corr, right]]
                    },
                );
                Ok(ArrayValue::Bool(flags.into_dyn()))
            }
            _ => Err(error()),
        }
    }

    pub(super) fn apply_categories(&self, input: ArrayView3<'_, bool>) -> ArrayValue {
        let (corr, channels, categories) = input.dim();
        let output = ndarray::Array3::from_shape_fn(
            (corr, self.samples.len(), categories).f(),
            |(corr, chan, category)| {
                let [left, right] = self.samples[chan].bracket.map(|index| self.physical(index));
                input[[corr, left.min(channels - 1), category]]
                    || input[[corr, right.min(channels - 1), category]]
            },
        );
        ArrayValue::Bool(output.into_dyn())
    }
}

trait SampleValue:
    Copy
    + Default
    + Add<Output = Self>
    + Sub<Output = Self>
    + Mul<f32, Output = Self>
    + Div<f32, Output = Self>
{
}
impl SampleValue for f32 {}
impl SampleValue for num_complex::Complex32 {}

fn second_derivatives<T: SampleValue>(
    n: usize,
    coordinate: impl Fn(usize) -> f64,
    get: impl Fn(usize) -> T,
    diagonal: &mut Vec<f32>,
    curvature: &mut Vec<T>,
) {
    diagonal.resize(n, 0.);
    curvature.resize(n, T::default());
    diagonal[0] = 0.;
    curvature[0] = T::default();
    curvature[n - 1] = T::default();
    for i in 1..n - 1 {
        let a = (coordinate(i) - coordinate(i - 1)) as f32;
        let c = (coordinate(i + 1) - coordinate(i)) as f32;
        let b = 2. * (a + c) - a * diagonal[i - 1];
        let gradient_diff = (get(i + 1) - get(i)) / c - (get(i) - get(i - 1)) / a;
        diagonal[i] = c / b;
        curvature[i] = (gradient_diff * 6. - curvature[i - 1] * a) / b;
    }
    for i in (2..n - 1).rev() {
        curvature[i] = curvature[i] - curvature[i + 1] * diagonal[i];
    }
}
