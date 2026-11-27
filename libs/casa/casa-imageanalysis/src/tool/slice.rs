// SPDX-License-Identifier: LGPL-3.0-or-later

//! 1D spatial slice extraction along pixel polylines (`ia.getslice`).

use casa_core::{Complex32, Complex64};
use casa_images::{PixelSlice, RegridMethod, get_slice};

use super::attachment::AttachedImage;
use super::error::{ImageToolError, ImageToolResult};
use super::handle::{ImageHandle, Storage, with_storage};

/// Multi-precision interpolated slice result returned by [`get_slice_attached`].
pub enum SliceRecordResult {
    /// Single-precision real slice.
    Float {
        /// Two resolved plane axis indices.
        axes: [i64; 2],
        /// Extracted slice values and positions.
        slice: PixelSlice<f32>,
    },
    /// Double-precision real slice.
    Double {
        /// Two resolved plane axis indices.
        axes: [i64; 2],
        /// Extracted slice values and positions.
        slice: PixelSlice<f64>,
    },
    /// Single-precision complex slice.
    Complex {
        /// Two resolved plane axis indices.
        axes: [i64; 2],
        /// Extracted slice values and positions.
        slice: PixelSlice<Complex32>,
    },
    /// Double-precision complex slice.
    DComplex {
        /// Two resolved plane axis indices.
        axes: [i64; 2],
        /// Extracted slice values and positions.
        slice: PixelSlice<Complex64>,
    },
}

/// Parses CASA's interpolation method string (`"nearest"`, `"linear"`, `"cubic"`).
pub fn parse_slice_method(method: &str) -> ImageToolResult<RegridMethod> {
    match method.as_bytes().first().map(u8::to_ascii_uppercase) {
        Some(b'N') => Ok(RegridMethod::Nearest),
        Some(b'L') => Ok(RegridMethod::Linear),
        Some(b'C') => Ok(RegridMethod::Cubic),
        _ => Err(ImageToolError::new("Illegal interpolation method")),
    }
}

/// Extracts an interpolated 1D slice using [`casa_images::get_slice`] (`ia.getslice`).
pub fn get_slice_attached(
    attached: &AttachedImage,
    x: &[f64],
    y: &[f64],
    axes: [i64; 2],
    coord: &[i64],
    npts: usize,
    method: RegridMethod,
) -> ImageToolResult<SliceRecordResult> {
    let mut image = attached.lock();
    attached.require_pixel_read_lock(&mut image)?;
    let result = match &*image {
        ImageHandle::Float(Storage::Temp {
            image,
            fits: Some(fits),
            ..
        }) => get_slice(&fits.image_view(image), x, y, axes, coord, npts, method)
            .map(|slice| SliceRecordResult::Float { axes, slice }),
        ImageHandle::Float(storage) => with_storage!(storage, view => {
            get_slice(view.as_ref(), x, y, axes, coord, npts, method)
                .map(|slice| SliceRecordResult::Float { axes, slice })
        }),
        ImageHandle::Double(storage) => with_storage!(storage, view => {
            get_slice(view.as_ref(), x, y, axes, coord, npts, method)
                .map(|slice| SliceRecordResult::Double { axes, slice })
        }),
        ImageHandle::Complex(storage) => with_storage!(storage, view => {
            get_slice(view.as_ref(), x, y, axes, coord, npts, method)
                .map(|slice| SliceRecordResult::Complex { axes, slice })
        }),
        ImageHandle::DComplex(storage) => with_storage!(storage, view => {
            get_slice(view.as_ref(), x, y, axes, coord, npts, method)
                .map(|slice| SliceRecordResult::DComplex { axes, slice })
        }),
    };
    let release = attached.release_requested_lock(&mut image);
    let value = result?;
    release?;
    Ok(value)
}
