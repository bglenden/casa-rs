// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility and noise projection used by the legacy SubMS channel averager.

use casa_core::ArrayValue;
use casa_tables::TableError;
use ndarray::{Array2, ArrayD, ArrayView2, Ix2};
use std::ops::{AddAssign, Div, Mul};

use casa_ms::schema::main_table::OptionalMainColumn;

use super::visibility_columns::VisibilityColumns;

pub(super) struct ChannelAverages<'a> {
    flags: ArrayView2<'a, bool>,
    spectrum: Option<ArrayView2<'a, f32>>,
    bins: &'a [Vec<usize>],
    totals: Array2<f64>,
}

impl<'a> ChannelAverages<'a> {
    pub(super) fn new(
        flags: &'a ArrayValue,
        spectrum: Option<&'a ArrayValue>,
        bins: &'a [Vec<usize>],
        flag_row: bool,
    ) -> Result<Self, TableError> {
        let ArrayValue::Bool(flags) = flags else {
            return Err(TableError::Schema("FLAG must hold Bool".into()));
        };
        let flags = flags.view().into_dimensionality::<Ix2>().map_err(|_| {
            TableError::Schema("FLAG must have correlation and channel axes".into())
        })?;
        super::validate_channel_bins(bins, flags.ncols())?;
        let spectrum = spectrum
            .map(|value| {
                if let ArrayValue::Float32(value) = value
                    && value.shape() == flags.shape()
                {
                    return Ok(value
                        .view()
                        .into_dimensionality::<Ix2>()
                        .expect("matching FLAG rank"));
                }
                Err(TableError::Schema("WEIGHT_SPECTRUM must match FLAG".into()))
            })
            .transpose()?;
        let totals = Array2::from_shape_fn((flags.nrows(), bins.len()), |(corr, bin)| {
            if flag_row {
                return 0.0;
            }
            bins[bin]
                .iter()
                .filter(|&&chan| !flags[[corr, chan]])
                .map(|&chan| {
                    spectrum
                        .as_ref()
                        .map_or(1.0, |weights| f64::from(weights[[corr, chan]]))
                })
                .sum()
        });
        Ok(Self {
            flags,
            spectrum,
            bins,
            totals,
        })
    }

    pub(super) fn flags(&self) -> ArrayValue {
        ArrayValue::Bool(self.totals.mapv(|weight| weight <= 0.0).into_dyn())
    }

    pub(super) fn weight_spectrum(&self) -> Option<ArrayValue> {
        self.spectrum
            .as_ref()
            .map(|_| ArrayValue::Float32(self.totals.mapv(|value| value as f32).into_dyn()))
    }

    pub(super) fn visibility(&self, value: ArrayValue) -> Result<ArrayValue, TableError> {
        match value {
            ArrayValue::Complex32(value) => self.mean(&value).map(ArrayValue::Complex32),
            ArrayValue::Float32(value) => self.mean(&value).map(ArrayValue::Float32),
            _ => Err(TableError::Schema(
                "visibility must hold Complex32 or Float32".into(),
            )),
        }
    }

    fn mean<T>(&self, input: &ArrayD<T>) -> Result<ArrayD<T>, TableError>
    where
        T: Copy + Default + AddAssign + Mul<f32, Output = T> + Div<f32, Output = T>,
    {
        if input.shape() != self.flags.shape() {
            return Err(TableError::Schema("visibility must match FLAG".into()));
        }
        Ok(Array2::from_shape_fn(self.totals.raw_dim(), |(corr, bin)| {
            let total = self.totals[[corr, bin]];
            if total <= 0.0 {
                return T::default();
            }
            let mut sum = T::default();
            for &chan in &self.bins[bin] {
                if !self.flags[[corr, chan]] {
                    let weight = self
                        .spectrum
                        .as_ref()
                        .map_or(1.0, |weights| weights[[corr, chan]]);
                    sum += input[[corr, chan]] * weight;
                }
            }
            sum / total as f32
        })
        .into_dyn())
    }
}

/// Scale each correlation by the width of the first bin. A shorter final
/// bin changes spectral weights but has no separate row WEIGHT or SIGMA.
pub(super) fn scale_channel_noise(
    columns: &VisibilityColumns,
    width: usize,
    weight: &mut ArrayValue,
    sigma: &mut ArrayValue,
) -> Result<(), TableError> {
    use OptionalMainColumn::{CorrectedData, Data, FloatData, ModelData};

    let contains = |name| columns.copies().iter().any(|column| column.source == name);
    let observed = contains(Data);
    let corrected = contains(CorrectedData);
    let model = contains(ModelData);
    let floating = contains(FloatData);
    let (ArrayValue::Float32(weight), ArrayValue::Float32(sigma)) = (weight, sigma) else {
        return Err(TableError::Schema(
            "WEIGHT and SIGMA must hold Float32".into(),
        ));
    };
    let width = width as f64;
    let root_width = width.sqrt();
    for (weight, sigma) in weight.iter_mut().zip(sigma) {
        if observed || floating {
            *sigma = (f64::from(*sigma) / root_width) as f32;
            if !corrected {
                *weight = if *sigma > 0.0 {
                    (1.0 / f64::from(*sigma).powi(2)) as f32
                } else {
                    f32::EPSILON
                };
            }
        }
        if corrected || model {
            *weight = (f64::from(*weight) * width) as f32;
            if !observed {
                *sigma = if *weight > 0.0 {
                    1.0 / weight.sqrt()
                } else {
                    f32::MAX
                };
            }
        }
    }
    Ok(())
}

/// Split persists weights from sigma, except when CORRECTED_DATA is written as DATA.
pub(super) fn align_row_noise(
    columns: &VisibilityColumns,
    weight: &mut ArrayValue,
    sigma: &mut ArrayValue,
) -> Result<(), TableError> {
    let (ArrayValue::Float32(weight), ArrayValue::Float32(sigma)) = (weight, sigma) else {
        return Err(TableError::Schema(
            "WEIGHT and SIGMA must hold Float32".into(),
        ));
    };
    if columns.corrected_becomes_data() {
        for (sigma, &weight) in sigma.iter_mut().zip(weight.iter()) {
            *sigma = if weight > 0.0 {
                (1.0 / f64::from(weight).sqrt()) as f32
            } else {
                f32::MAX
            };
        }
    } else {
        for (weight, &sigma) in weight.iter_mut().zip(sigma.iter()) {
            *weight = if sigma > 0.0 {
                (1.0 / f64::from(sigma).powi(2)) as f32
            } else {
                f32::EPSILON
            };
        }
    }
    Ok(())
}
