// SPDX-License-Identifier: LGPL-3.0-or-later

//! Pixel array chunk extraction and writing (`ia.getchunk`, `ia.putchunk`, `ia.set`).

use casa_core::{Complex32, Complex64};
use casa_images::{ImageInterface, ImagePixel};
use casa_lattices::Lattice;
use ndarray::{ArrayD, IxDyn, Zip};

use super::attachment::AttachedImage;
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, Storage, with_storage};

/// Multi-dimensional pixel buffer tagged with its CASA pixel type.
#[derive(Debug, Clone)]
pub enum PixelChunk {
    /// Single-precision real pixels.
    Float(ArrayD<f32>),
    /// Double-precision real pixels.
    Double(ArrayD<f64>),
    /// Single-precision complex pixels.
    Complex(ArrayD<Complex32>),
    /// Double-precision complex pixels.
    DComplex(ArrayD<Complex64>),
}

impl PixelChunk {
    /// Returns the array dimensionality.
    pub fn shape(&self) -> &[usize] {
        match self {
            Self::Float(a) => a.shape(),
            Self::Double(a) => a.shape(),
            Self::Complex(a) => a.shape(),
            Self::DComplex(a) => a.shape(),
        }
    }
}

/// Extracted pixel chunk and optional validity mask returned by `ia.getchunk`.
#[derive(Debug, Clone)]
pub struct ChunkReadResult {
    /// Pixel values in shape order (with degenerate axes optionally dropped).
    pub pixels: PixelChunk,
    /// Mask values when `getmask=true`.
    pub mask: Option<ArrayD<bool>>,
    /// Diagnostic log lines when `list=true`.
    pub logs: Vec<ToolLogLine>,
}

/// Parameters for `ia.getchunk`.
#[derive(Debug, Clone, Default)]
pub struct GetChunkRequest {
    /// Lower inclusive pixel corner (`-1` uses `0`).
    pub blc: Vec<isize>,
    /// Upper inclusive pixel corner (`-1` uses `shape[i] - 1`).
    pub trc: Vec<isize>,
    /// Stride along each axis (`>= 1`).
    pub inc: Vec<isize>,
    /// Optional axes along which to slice (`axes` in CASA).
    pub axes: Vec<usize>,
    /// Log bounding box summary when `true`.
    pub list: bool,
    /// Drop trailing/singleton axes of length 1.
    pub dropdeg: bool,
    /// Return pixel mask array instead of/alongside pixels.
    pub getmask: bool,
}

/// Validates and resolves `blc`, `trc`, `inc` bounds against `image_shape`.
pub fn resolve_chunk_bounds(
    image_shape: &[usize],
    blc: &[isize],
    trc: &[isize],
    inc: &[isize],
) -> ImageToolResult<(Vec<usize>, Vec<usize>, Vec<usize>)> {
    let ndim = image_shape.len();
    let mut start = vec![0usize; ndim];
    let mut count = vec![0usize; ndim];
    let mut stride = vec![1usize; ndim];

    for axis in 0..ndim {
        let len = image_shape[axis] as isize;
        let b = blc.get(axis).copied().unwrap_or(-1);
        let b = if b < 0 { 0 } else { b };
        let t = trc.get(axis).copied().unwrap_or(-1);
        let t = if t < 0 { len - 1 } else { t };
        let step = inc.get(axis).copied().unwrap_or(1);
        if step <= 0 {
            return Err(ImageToolError::new(format!(
                "Increment must be positive on axis {axis}, got {step}."
            )));
        }
        if b >= len || t >= len || b > t {
            return Err(ImageToolError::new(format!(
                "Chunk bounds [{b}..{t}] out of range for axis {axis} of length {len}."
            )));
        }
        start[axis] = b as usize;
        stride[axis] = step as usize;
        count[axis] = ((t - b) / step + 1) as usize;
    }
    Ok((start, count, stride))
}

/// Reads pixels (and optionally mask) from `attached` according to `request`.
pub fn get_chunk(
    attached: &AttachedImage,
    request: &GetChunkRequest,
) -> ImageToolResult<ChunkReadResult> {
    let mut image = attached.lock();
    attached.require_pixel_read_lock(&mut image)?;
    let shape = image.shape().to_vec();
    let (start, count, stride) =
        resolve_chunk_bounds(&shape, &request.blc, &request.trc, &request.inc)?;
    let mut logs = Vec::new();
    if request.list {
        let end: Vec<usize> = start
            .iter()
            .zip(count.iter())
            .zip(stride.iter())
            .map(|((&s, &c), &st)| s + c.saturating_sub(1) * st)
            .collect();
        logs.push(ToolLogLine::info(format!(
            "Reading chunk blc={start:?} trc={end:?} inc={stride:?}"
        )));
    }

    let (pixels, mask) = match &*image {
        ImageHandle::Float(storage) => {
            if let Some(fits) = image.fits() {
                let metadata = match storage {
                    Storage::Temp { image, .. } => image.as_ref(),
                    Storage::Paged(_) => unreachable!(),
                };
                let view = fits.image_view(metadata);
                let arr = Lattice::get_slice(&view, &start, &count, &stride)?;
                let m = if request.getmask {
                    Some(
                        view.get_mask_slice(&start, &count, &stride)?
                            .unwrap_or_else(|| ArrayD::from_elem(IxDyn(arr.shape()), true)),
                    )
                } else {
                    None
                };
                (PixelChunk::Float(arr), m)
            } else {
                with_storage!(storage, img => {
                    let (arr, m) = read_typed_chunk(img.as_ref(), &start, &count, &stride, request.getmask)?;
                    (PixelChunk::Float(arr), m)
                })
            }
        }
        ImageHandle::Double(storage) => with_storage!(storage, img => {
            let (arr, m) = read_typed_chunk(img.as_ref(), &start, &count, &stride, request.getmask)?;
            (PixelChunk::Double(arr), m)
        }),
        ImageHandle::Complex(storage) => with_storage!(storage, img => {
            let (arr, m) = read_typed_chunk(img.as_ref(), &start, &count, &stride, request.getmask)?;
            (PixelChunk::Complex(arr), m)
        }),
        ImageHandle::DComplex(storage) => with_storage!(storage, img => {
            let (arr, m) = read_typed_chunk(img.as_ref(), &start, &count, &stride, request.getmask)?;
            (PixelChunk::DComplex(arr), m)
        }),
    };

    let pixels = if request.dropdeg {
        drop_degenerate_chunk(pixels)
    } else {
        pixels
    };
    let mask = if request.dropdeg {
        mask.map(drop_degenerate_mask)
    } else {
        mask
    };

    Ok(ChunkReadResult { pixels, mask, logs })
}

/// Writes `pixels` into `attached` starting at `blc` (`putchunk`).
pub fn put_chunk(
    attached: &AttachedImage,
    pixels: &PixelChunk,
    blc: &[isize],
    inc: &[isize],
    list: bool,
    replicate: bool,
    record_history: bool,
) -> ImageToolResult<Vec<ToolLogLine>> {
    let mut image = attached.lock();
    attached.require_write_lock(&mut image, "putchunk")?;
    let image_shape = image.shape().to_vec();
    let start: Vec<usize> = (0..image_shape.len())
        .map(|axis| {
            let v = blc.get(axis).copied().unwrap_or(0);
            if v < 0 { 0usize } else { v as usize }
        })
        .collect();
    for (&s, &dim) in start.iter().zip(image_shape.iter()) {
        if s >= dim {
            return Err(ImageToolError::new(format!(
                "blc {start:?} exceeds image shape {image_shape:?}"
            )));
        }
    }
    let _ = (inc, replicate);
    let mut logs = Vec::new();
    if list {
        logs.push(ToolLogLine::info(format!(
            "Writing chunk of shape {:?} at blc={start:?}",
            pixels.shape()
        )));
    }

    match (&mut *image, pixels) {
        (ImageHandle::Float(storage), PixelChunk::Float(arr)) => {
            put_typed_chunk(storage, arr, &start)?;
        }
        (ImageHandle::Float(storage), PixelChunk::Double(arr)) => {
            let casted = arr.mapv(|v| v as f32);
            put_typed_chunk(storage, &casted, &start)?;
        }
        (ImageHandle::Double(storage), PixelChunk::Double(arr)) => {
            put_typed_chunk(storage, arr, &start)?;
        }
        (ImageHandle::Double(storage), PixelChunk::Float(arr)) => {
            let casted = arr.mapv(f64::from);
            put_typed_chunk(storage, &casted, &start)?;
        }
        (ImageHandle::Complex(storage), PixelChunk::Complex(arr)) => {
            put_typed_chunk(storage, arr, &start)?;
        }
        (ImageHandle::Complex(storage), PixelChunk::DComplex(arr)) => {
            let casted = arr.mapv(|v| Complex32::new(v.re as f32, v.im as f32));
            put_typed_chunk(storage, &casted, &start)?;
        }
        (ImageHandle::DComplex(storage), PixelChunk::DComplex(arr)) => {
            put_typed_chunk(storage, arr, &start)?;
        }
        (ImageHandle::DComplex(storage), PixelChunk::Complex(arr)) => {
            let casted = arr.mapv(|v| Complex64::new(v.re.into(), v.im.into()));
            put_typed_chunk(storage, &casted, &start)?;
        }
        _ => {
            return Err(ImageToolError::new(
                "Pixel array type is incompatible with target image (real vs complex).",
            ));
        }
    }

    if record_history {
        image.record_history("putchunk", format!("ia.putchunk(blc={blc:?}, inc={inc:?})"))?;
    }
    image.save()?;
    Ok(logs)
}

fn read_typed_chunk<T: ImagePixel + casa_lattices::LatticeElement>(
    image: &impl ImageInterface<T>,
    start: &[usize],
    count: &[usize],
    stride: &[usize],
    getmask: bool,
) -> ImageToolResult<(ArrayD<T>, Option<ArrayD<bool>>)> {
    let arr = Lattice::get_slice(image, start, count, stride)?;
    let mask = if getmask {
        Some(
            image
                .get_mask_slice(start, count, stride)?
                .unwrap_or_else(|| ArrayD::from_elem(IxDyn(arr.shape()), true)),
        )
    } else {
        None
    };
    Ok((arr, mask))
}

fn put_typed_chunk<T: ImagePixel>(
    storage: &mut Storage<T>,
    arr: &ArrayD<T>,
    start: &[usize],
) -> ImageToolResult<()> {
    let mut padded = arr.view();
    let target_ndim = match storage {
        Storage::Paged(paged) => paged.shape().len(),
        Storage::Temp { image, .. } => image.shape().len(),
    };
    while padded.ndim() < target_ndim {
        let axis = ndarray::Axis(padded.ndim());
        padded = padded.insert_axis(axis);
    }
    let owned = padded.to_owned();
    match storage {
        Storage::Paged(paged) => paged.put_slice(&owned, start)?,
        Storage::Temp { image, .. } => image.put_slice(&owned, start)?,
    }
    Ok(())
}

fn drop_degenerate_chunk(chunk: PixelChunk) -> PixelChunk {
    match chunk {
        PixelChunk::Float(a) => PixelChunk::Float(drop_degenerate_array(a)),
        PixelChunk::Double(a) => PixelChunk::Double(drop_degenerate_array(a)),
        PixelChunk::Complex(a) => PixelChunk::Complex(drop_degenerate_array(a)),
        PixelChunk::DComplex(a) => PixelChunk::DComplex(drop_degenerate_array(a)),
    }
}

fn drop_degenerate_mask(mask: ArrayD<bool>) -> ArrayD<bool> {
    drop_degenerate_array(mask)
}

pub(crate) fn drop_degenerate_array<A>(mut array: ArrayD<A>) -> ArrayD<A> {
    for axis in (0..array.ndim()).rev() {
        if array.ndim() == 1 {
            break;
        }
        if array.shape()[axis] == 1 {
            array = array.index_axis_move(ndarray::Axis(axis), 0);
        }
    }
    array
}

/// Element-wise logical AND between an optional image/region mask and an optional expression mask.
pub fn and_mask(
    original: Option<ArrayD<bool>>,
    expression: Option<ArrayD<bool>>,
) -> Option<ArrayD<bool>> {
    match (original, expression) {
        (Some(mut original), Some(expression)) => {
            Zip::from(&mut original)
                .and(&expression)
                .for_each(|valid, &passes| *valid &= passes);
            Some(original)
        }
        (original, expression) => original.or(expression),
    }
}

/// Validates CASA reduction/collapse axis indices against image `rank`.
///
/// A single negative entry (e.g. `[-1]`) denotes collapsing all axes (`Ok(vec![])`).
pub fn collapse_axes(axes: &[i64], rank: usize) -> ImageToolResult<Vec<usize>> {
    if matches!(axes, [only] if *only < 0) {
        return Ok(Vec::new());
    }
    let mut collapse = Vec::with_capacity(axes.len());
    for &axis in axes {
        if axis < 0 || axis as usize >= rank || collapse.contains(&(axis as usize)) {
            return Err(ImageToolError::new(
                "IPosition::makeAxisPath: invalid defined axes",
            ));
        }
        collapse.push(axis as usize);
    }
    Ok(collapse)
}
