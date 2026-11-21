// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pixel arithmetic used by the task for real and complex interpolation.

use std::ops::{Add, Div, Mul, Sub};

use ndarray::NdFloat;
use num_complex::Complex;

use crate::ImagePixel;

/// Pixel arithmetic for separable and two-dimensional image interpolation.
pub trait RegridPixel:
    ImagePixel
    + Copy
    + Add<Output = Self>
    + Sub<Output = Self>
    + Mul<Self::Real, Output = Self>
    + Div<Self::Real, Output = Self>
{
    /// Scalar arithmetic precision used by the stored pixel type.
    type Real: NdFloat;
    /// The number of independent scalar components in a pixel.
    const COMPONENTS: usize;

    /// The requested real or imaginary component converted to f64.
    fn component(self, index: usize) -> f64;
    /// Replace the requested component, preserving the other component.
    fn with_component(self, index: usize, value: f64) -> Self;
}

macro_rules! impl_regrid_pixels {
    ($real:ty) => {
        impl RegridPixel for $real {
            type Real = $real;
            const COMPONENTS: usize = 1;

            fn component(self, _index: usize) -> f64 {
                f64::from(self)
            }

            fn with_component(self, _index: usize, value: f64) -> Self {
                value as Self
            }
        }

        impl RegridPixel for Complex<$real> {
            type Real = $real;
            const COMPONENTS: usize = 2;

            fn component(self, index: usize) -> f64 {
                f64::from(if index == 0 { self.re } else { self.im })
            }

            fn with_component(mut self, index: usize, value: f64) -> Self {
                if index == 0 {
                    self.re = value as $real;
                } else {
                    self.im = value as $real;
                }
                self
            }
        }
    };
}

impl_regrid_pixels!(f32);
impl_regrid_pixels!(f64);
