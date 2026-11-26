// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fractional-channel time-dependent shift on a uniform reference grid.

use std::f64::consts::TAU;

use casa_core::ArrayValue;
use casa_fft::Fft2;
use casa_tables::TableError;
use ndarray::Ix2;
use num_complex::Complex32;

#[derive(Clone, Copy)]
pub(super) struct Shift {
    relative: f64,
}

impl Shift {
    pub(super) fn new(relative: f64) -> Result<Option<Self>, TableError> {
        if relative == 0. {
            return Ok(None);
        }
        if relative.abs() >= 1. || !relative.is_finite() {
            return Err(TableError::Schema(
                "The FFT shift exceeds the regridded bandwidth.".into(),
            ));
        }
        Ok(Some(Self { relative }))
    }

    pub(super) fn apply(
        &self,
        value: ArrayValue,
        pre_shift_flags: Option<&ArrayValue>,
    ) -> Result<ArrayValue, TableError> {
        let error = || {
            TableError::Schema(
                "FFT shift needs matching rank-two visibility and FLAG cells.".into(),
            )
        };
        if let ArrayValue::Bool(flags) = value {
            let mut flags = flags.into_dimensionality::<Ix2>().map_err(|_| error())?;
            for mut row in flags.rows_mut() {
                let n = row.len();
                let upper = (n as f64 * self.relative.abs()).ceil() as usize;
                let lower = (n as f64 * self.relative.abs()).floor() as usize;
                let index = |i| if self.relative > 0. { i } else { n - 1 - i };
                for i in (0..n - upper).rev() {
                    if row[index(i)] {
                        row[index(i + upper)] = true;
                        row[index(i)] = false;
                        if lower < upper {
                            row[index(i + lower)] = true;
                        }
                    }
                }
                for i in 0..upper {
                    row[index(i)] = true;
                }
            }
            return Ok(ArrayValue::Bool(flags.into_dyn()));
        }
        let Some(ArrayValue::Bool(flags)) = pre_shift_flags else {
            return Err(error());
        };
        let flags = flags
            .view()
            .into_dimensionality::<Ix2>()
            .map_err(|_| error())?;
        if flags.shape() != value.shape() {
            return Err(error());
        }
        let n = flags.ncols();
        let mut fft = Fft2::<f32>::with_threads([1, n], 1)
            .map_err(fft_error)?
            .with_estimated_plan();
        let mut work = vec![Complex32::default(); n];
        match value {
            ArrayValue::Float32(data) => {
                let mut values = data.into_dimensionality::<Ix2>().map_err(|_| error())?;
                for (mut row, mask) in values.rows_mut().into_iter().zip(flags.rows()) {
                    for (sample, &value) in work.iter_mut().zip(row.iter()) {
                        *sample = Complex32::new(value, 0.);
                    }
                    self.lane(&mut fft, &mut work, mask.iter().copied(), true)?;
                    for (value, sample) in row.iter_mut().zip(&work) {
                        *value = sample.re;
                    }
                }
                Ok(ArrayValue::Float32(values.into_dyn()))
            }
            ArrayValue::Complex32(data) => {
                let mut values = data.into_dimensionality::<Ix2>().map_err(|_| error())?;
                for (mut row, mask) in values.rows_mut().into_iter().zip(flags.rows()) {
                    for (sample, &value) in work.iter_mut().zip(row.iter()) {
                        *sample = value;
                    }
                    self.lane(&mut fft, &mut work, mask.iter().copied(), false)?;
                    for (value, sample) in row.iter_mut().zip(&work) {
                        *value = *sample;
                    }
                }
                Ok(ArrayValue::Complex32(values.into_dyn()))
            }
            _ => Err(error()),
        }
    }

    fn lane(
        &self,
        fft: &mut Fft2<f32>,
        work: &mut [Complex32],
        mask: impl Iterator<Item = bool>,
        real: bool,
    ) -> Result<(), TableError> {
        let n = work.len();
        for (i, flagged) in mask.enumerate().take(n - 1) {
            if flagged {
                work[i] = if i == 0 {
                    work[1]
                } else {
                    (work[i - 1] + work[i + 1]) * 0.5
                };
            }
        }
        fft.transform(work, !real).map_err(fft_error)?;
        let sign = if real { -1. } else { 1. };
        for (i, sample) in work.iter_mut().enumerate() {
            let harmonic = ((i + n / 2) % n) as f64 - (n / 2) as f64;
            let phase = (sign * TAU * self.relative * harmonic) as f32;
            *sample *= Complex32::from_polar(1., phase);
        }
        fft.transform(work, real).map_err(fft_error)?;
        for sample in work {
            *sample /= n as f32;
        }
        Ok(())
    }
}

fn fft_error(error: casa_fft::FftError) -> TableError {
    TableError::Schema(error.to_string())
}
