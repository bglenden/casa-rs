// SPDX-License-Identifier: LGPL-3.0-or-later
//! The spline used by the legacy CASA Fourier machine to interpolate the
//! degridded model from its image channels to native visibility channels.

use num_complex::Complex32;

use super::{LegacyPredictionMap, SpectralAxis, SpectralKernel, SpectralResampler};

impl SpectralResampler {
    /// Legacy spline prediction uses the cubic channel map and interpolates
    /// the original image frequencies with casacore's second derivatives.
    /// Use the row prediction methods for this resampler. The bounded
    /// backend stencil cannot express a spline across the whole image axis.
    #[must_use]
    pub fn legacy_spline(output: SpectralAxis, matching: SpectralAxis) -> Self {
        let mut resampler = Self::channel_local(output, SpectralKernel::Cubic);
        resampler.legacy_prediction_map = Some(LegacyPredictionMap {
            axis: matching,
            selection: None,
            spline: true,
        });
        resampler
    }

    pub(super) fn uses_legacy_spline(&self) -> bool {
        self.legacy_prediction_map
            .as_ref()
            .is_some_and(|mapping| mapping.spline)
    }
}

#[derive(Debug, Default)]
pub(super) struct SplineScratch {
    upper: Vec<f64>,
    derivatives: Vec<Complex32>,
}

impl SplineScratch {
    pub(super) fn with_capacity(planes: usize, npol: usize) -> Self {
        Self {
            upper: Vec::with_capacity(planes),
            derivatives: Vec::with_capacity(planes * npol),
        }
    }

    pub(super) const fn bytes(planes: usize, npol: usize) -> u64 {
        (planes * (size_of::<f64>() + npol * size_of::<Complex32>())) as u64
    }

    pub(super) fn capacity_bytes(&self) -> u64 {
        (self.upper.capacity() * size_of::<f64>()
            + self.derivatives.capacity() * size_of::<Complex32>()) as u64
    }

    pub(super) fn prepare(
        &mut self,
        axis: &SpectralAxis,
        npol: usize,
        value: impl Fn(usize, usize) -> Complex32,
    ) {
        let planes = axis.channels as usize;
        self.upper.clear();
        self.upper.resize(planes, 0.0);
        self.derivatives.clear();
        self.derivatives.resize(planes * npol, Complex32::default());

        let mut width = axis.centre_hz(1) - axis.centre_hz(0);
        for index in 1..planes - 1 {
            let previous_width = width;
            width = axis.centre_hz(index as u32 + 1) - axis.centre_hz(index as u32);
            let diagonal = 2.0 * (previous_width + width) - previous_width * self.upper[index - 1];
            self.upper[index] = width / diagonal;
            for pol in 0..npol {
                let current = value(index, pol);
                let right_slope = (value(index + 1, pol) - current) * (1.0 / width) as f32;
                let left_slope = (current - value(index - 1, pol)) * (1.0 / previous_width) as f32;
                let previous = self.derivatives[(index - 1) * npol + pol];
                self.derivatives[index * npol + pol] = ((right_slope - left_slope) * 6.0
                    - previous * previous_width as f32)
                    * (1.0 / diagonal) as f32;
            }
        }
        // InterpolateArray1D's back substitution stops before plane 1.
        for index in (2..planes - 1).rev() {
            for pol in 0..npol {
                let right = self.derivatives[(index + 1) * npol + pol];
                self.derivatives[index * npol + pol] -= right * self.upper[index] as f32;
            }
        }
    }

    pub(super) fn evaluate(
        &self,
        axis: &SpectralAxis,
        frequency: f64,
        value: impl Fn(usize, usize) -> Complex32,
        flags: &[bool],
        output: &mut [Complex32],
    ) {
        let npol = output.len();
        let position = axis
            .tabular_pixel(frequency)
            .unwrap_or_else(|| (frequency - axis.first_hz) / axis.increment_hz);
        let right = (position.ceil() as usize).clamp(1, axis.channels as usize - 1);
        let left = right - 1;
        let left_hz = axis.centre_hz(left as u32);
        let right_hz = axis.centre_hz(right as u32);
        let width = right_hz - left_hz;
        let a = (right_hz - frequency) / width;
        let b = 1.0 - a;
        let scale = width * width / 6.0;
        let second_left = (scale * (a * a * a - a)) as f32;
        let second_right = (scale * (b * b * b - b)) as f32;
        for (pol, cell) in output.iter_mut().enumerate() {
            if !flags[pol] {
                *cell = value(left, pol) * a as f32
                    + value(right, pol) * b as f32
                    + self.derivatives[left * npol + pol] * second_left
                    + self.derivatives[right * npol + pol] * second_right;
            }
        }
    }
}
