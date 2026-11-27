// SPDX-License-Identifier: LGPL-3.0-or-later

//! 1D spatial slice extraction along pixel polylines (`ia.getslice`).

use casa_core::{Complex32, Complex64};
use casa_images::{ImageInterface, PixelSlice, RegridMethod, get_slice};
use casa_lattices::Lattice;
use ndarray::IxDyn;

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

/// Parameters for `ia.getslice`.
#[derive(Debug, Clone)]
pub struct GetSliceRequest<'a> {
    /// Vertex x pixel coordinates (at least 2 points).
    pub x: &'a [f64],
    /// Vertex y pixel coordinates (matching `x.len()`).
    pub y: &'a [f64],
    /// Fixed slice coordinates on remaining axes.
    pub coord: &'a [usize],
    /// Interpolation method (`"nearest"`, `"linear"`, `"cubic"`).
    pub method: &'a str,
    /// Number of sample points along the polyline (`0` chooses automatically).
    pub npts: usize,
    /// Optional mask expression.
    pub mask: Option<&'a str>,
}

/// 1D slice values and geometry returned by `ia.getslice`.
#[derive(Debug, Clone)]
pub struct SliceProfileResult {
    /// Sampled pixel values (`f64`).
    pub pixel: Vec<f64>,
    /// Validity mask along the profile.
    pub mask: Vec<bool>,
    /// Cumulative arc-length pixel distance from start vertex.
    pub distance: Vec<f64>,
    /// X pixel coordinate at each sample.
    pub xpos: Vec<f64>,
    /// Y pixel coordinate at each sample.
    pub ypos: Vec<f64>,
    /// Interpolation method name echoed back.
    pub method: String,
    /// Diagnostic messages.
    pub axes: [usize; 2],
}

/// Extracts an interpolated 1D profile along `(request.x, request.y)` from `attached`.
pub fn extract_slice(
    attached: &AttachedImage,
    request: &GetSliceRequest<'_>,
) -> ImageToolResult<SliceProfileResult> {
    if request.x.len() < 2 || request.x.len() != request.y.len() {
        return Err(ImageToolError::new(
            "getslice requires x and y vectors of equal length >= 2.",
        ));
    }
    let method = request.method.trim().to_ascii_lowercase();
    if !matches!(method.as_str(), "" | "nearest" | "linear" | "cubic") {
        return Err(ImageToolError::new(format!(
            "Unsupported getslice interpolation method '{}'.",
            request.method
        )));
    }
    let _ = request.mask;

    let mut image = attached.lock();
    attached.require_pixel_read_lock(&mut image)?;
    let shape = image.shape().to_vec();
    if shape.len() < 2 {
        return Err(ImageToolError::new(
            "getslice requires an image with at least 2 axes.",
        ));
    }

    let mut total_len = 0.0_f64;
    for idx in 1..request.x.len() {
        let dx = request.x[idx] - request.x[idx - 1];
        let dy = request.y[idx] - request.y[idx - 1];
        total_len += dx.hypot(dy);
    }
    let n_samples = if request.npts > 0 {
        request.npts
    } else {
        (total_len.ceil() as usize + 1).max(2)
    };

    let mut xpos = Vec::with_capacity(n_samples);
    let mut ypos = Vec::with_capacity(n_samples);
    let mut distance = Vec::with_capacity(n_samples);
    for s_idx in 0..n_samples {
        let target_d = if n_samples <= 1 {
            0.0
        } else {
            total_len * (s_idx as f64) / ((n_samples - 1) as f64)
        };
        let (px, py) = interpolate_polyline(request.x, request.y, target_d);
        xpos.push(px);
        ypos.push(py);
        distance.push(target_d);
    }

    let mut base_coord = vec![0usize; shape.len()];
    for axis in 2..shape.len() {
        let pos = request.coord.get(axis).copied().unwrap_or(0);
        if pos >= shape[axis] {
            return Err(ImageToolError::new(format!(
                "Slice coordinate {pos} out of bounds on axis {axis} (length {}).",
                shape[axis]
            )));
        }
        base_coord[axis] = pos;
    }

    let mut start = vec![0usize; shape.len()];
    let mut count = vec![1usize; shape.len()];
    start[0] = 0;
    start[1] = 0;
    count[0] = shape[0];
    count[1] = shape[1];
    for axis in 2..shape.len() {
        start[axis] = base_coord[axis];
        count[axis] = 1;
    }
    let stride = vec![1usize; shape.len()];

    let (plane, mask_plane) = match &*image {
        ImageHandle::Float(storage) => {
            if let Some(fits) = image.fits() {
                let metadata = match storage {
                    Storage::Temp { image, .. } => image.as_ref(),
                    Storage::Paged(_) => unreachable!(),
                };
                let view = fits.image_view(metadata);
                let arr = Lattice::get_slice(&view, &start, &count, &stride)?.mapv(f64::from);
                let m = view.get_mask_slice(&start, &count, &stride)?;
                (arr, m)
            } else {
                with_storage!(storage, img => {
                    let arr = img.get_slice(&start, &count)?.mapv(f64::from);
                    let m = img.get_mask_slice(&start, &count, &stride)?;
                    (arr, m)
                })
            }
        }
        ImageHandle::Double(storage) => with_storage!(storage, img => {
            let arr = img.get_slice(&start, &count)?;
            let m = img.get_mask_slice(&start, &count, &stride)?;
            (arr, m)
        }),
        ImageHandle::Complex(_) | ImageHandle::DComplex(_) => {
            return Err(ImageToolError::new(
                "Exception: getslice does not support complex images.",
            ));
        }
    };

    let nx = shape[0];
    let ny = shape[1];
    let mut pixel = Vec::with_capacity(n_samples);
    let mut mask = Vec::with_capacity(n_samples);
    let use_linear = method != "nearest";

    for (&px, &py) in xpos.iter().zip(ypos.iter()) {
        let idx_prefix = vec![0usize; shape.len()];
        let val_at = |ix: usize, iy: usize| -> (f64, bool) {
            let mut idx = idx_prefix.clone();
            idx[0] = ix;
            idx[1] = iy;
            let v = plane[IxDyn(&idx)];
            let m = mask_plane.as_ref().is_none_or(|mp| mp[IxDyn(&idx)]) && v.is_finite();
            (v, m)
        };
        if px < -0.5 || py < -0.5 || px > (nx as f64) - 0.5 || py > (ny as f64) - 0.5 {
            pixel.push(0.0);
            mask.push(false);
            continue;
        }
        if !use_linear {
            let ix = (px.round() as isize).clamp(0, nx as isize - 1) as usize;
            let iy = (py.round() as isize).clamp(0, ny as isize - 1) as usize;
            let (v, m) = val_at(ix, iy);
            pixel.push(v);
            mask.push(m);
        } else {
            let x0 = (px.floor() as isize).clamp(0, nx as isize - 1) as usize;
            let y0 = (py.floor() as isize).clamp(0, ny as isize - 1) as usize;
            let x1 = (x0 + 1).min(nx - 1);
            let y1 = (y0 + 1).min(ny - 1);
            let fx = (px - x0 as f64).clamp(0.0, 1.0);
            let fy = (py - y0 as f64).clamp(0.0, 1.0);
            let (v00, m00) = val_at(x0, y0);
            let (v10, m10) = val_at(x1, y0);
            let (v01, m01) = val_at(x0, y1);
            let (v11, m11) = val_at(x1, y1);
            if m00 && m10 && m01 && m11 {
                let v0 = v00 * (1.0 - fx) + v10 * fx;
                let v1 = v01 * (1.0 - fx) + v11 * fx;
                pixel.push(v0 * (1.0 - fy) + v1 * fy);
                mask.push(true);
            } else {
                let ix = (px.round() as isize).clamp(0, nx as isize - 1) as usize;
                let iy = (py.round() as isize).clamp(0, ny as isize - 1) as usize;
                let (v, m) = val_at(ix, iy);
                pixel.push(v);
                mask.push(m);
            }
        }
    }

    Ok(SliceProfileResult {
        pixel,
        mask,
        distance,
        xpos,
        ypos,
        method: if method.is_empty() {
            "linear".to_owned()
        } else {
            method
        },
        axes: [0, 1],
    })
}

fn interpolate_polyline(xs: &[f64], ys: &[f64], target_dist: f64) -> (f64, f64) {
    let mut accum = 0.0_f64;
    for i in 1..xs.len() {
        let seg = (xs[i] - xs[i - 1]).hypot(ys[i] - ys[i - 1]);
        if accum + seg >= target_dist && seg > 0.0 {
            let t = ((target_dist - accum) / seg).clamp(0.0, 1.0);
            return (
                xs[i - 1] + t * (xs[i] - xs[i - 1]),
                ys[i - 1] + t * (ys[i] - ys[i - 1]),
            );
        }
        accum += seg;
    }
    (*xs.last().unwrap_or(&0.0), *ys.last().unwrap_or(&0.0))
}
